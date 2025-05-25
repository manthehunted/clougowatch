package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const shortForm = "2006-01-02"
const MAX_RETRY = 3
const QUERYID = "queryId"

// AWS
var AwsConfig *aws.Config

func load() (aws.Config, *string) {
	// Load the Shared AWS Configuration (~/.aws/config)
	fmt.Println("config")
	cfg, err := config.LoadDefaultConfig(context.TODO())
	fmt.Println("config1")
	if err != nil {
		fmt.Println(err)
		log.Err(err)
		os.Exit(1)
	}
	client := sts.NewFromConfig(cfg)
	input := &sts.GetCallerIdentityInput{}
	result, err := client.GetCallerIdentity(context.TODO(), input)
	fmt.Println("config2")
	if err != nil {
		fmt.Println(err)
		log.Err(err)
		os.Exit(1)
	}
	return cfg, result.Account
}

// End AWS

// Logging
var LogLevel *zerolog.Level
var EnvLogLevel = map[string]zerolog.Level{
	"debug": zerolog.DebugLevel,
	"info":  zerolog.InfoLevel,
	"warn":  zerolog.WarnLevel,
	"error": zerolog.ErrorLevel,
}

type Logging interface {
	Debug(string)
	Info(string)
	Warning(string)
	Error(string)
}

type Logger struct {
	logger *zerolog.Logger
}

func (l Logger) Debug(s string) {
	l.logger.Debug().Msg(fmt.Sprintf("%s\n", s))
}

func (l Logger) Info(s string) {
	l.logger.Info().Msg(fmt.Sprintf("%s\n", s))
}

func (l Logger) Warning(s string) {
	l.logger.Warn().Msg(fmt.Sprintf("%s\n", s))
}

func (l Logger) Error(s string) {
	l.logger.Error().Msg(fmt.Sprintf("%s\n", s))
}

// End Logging

// Query
type Query string

func (q *Query) check() bool {
	s := *q
	return strings.Contains(string(s), "stats")
}

func (q *Query) add() string {
	s := string(*q)
	s += "| stats count()"
	return s
}

// End Query

// Parameters
type TimeParam struct {
	count int
	max   *int
}

type CloudWatchParams struct {
	queryInput           cloudwatchlogs.StartQueryInput
	timeInput            *TimeParam
	queryId              *string
	fetchAllLengthyQuery bool
}

func (p *CloudWatchParams) AssignId(queryid *string) {
	p.queryId = queryid
}

func (c *CloudWatchParams) Assign(b Bound) {
	c.queryInput.StartTime = &b.start
	c.queryInput.EndTime = &b.end
}

func parseTimeParams(args []string) TimeParam {
	var p TimeParam

	for idx := 0; idx < len(args); idx += 2 {
		switch args[idx] {
		case "-w", "--min-wait":
			v := args[idx+1]
			i, e := strconv.Atoi(v)
			if e == nil {
				p.max = &i
				continue
			}
		}
	}
	return p
}

func parseLengthyQuery(args []string) bool {
	// if yes, get all logs for lengthy cloudwatch logs
	var isYes bool
	for idx := 0; idx < len(args); idx += 2 {
		switch args[idx] {
		case "-y":
			isYes = true
			return isYes
		}
	}
	return isYes
}

func parseParams(args []string) CloudWatchParams {
	queryInput := parseCloudParams(args)
	timeParams := parseTimeParams(args)
	fetchAllLengthyQuery := parseLengthyQuery(args)
	return CloudWatchParams{queryInput: queryInput, timeInput: &timeParams, fetchAllLengthyQuery: fetchAllLengthyQuery}
}

func parseCloudParams(args []string) cloudwatchlogs.StartQueryInput {
	inputs := cloudwatchlogs.StartQueryInput{}
	for idx := 0; idx < len(args); idx += 2 {
		switch args[idx] {
		case "-q", "--query":
			//QueryString *string
			// handle double quote gracefully
			v := strings.ReplaceAll(args[idx+1], `"`, "\x22")
			inputs.QueryString = &v
		case "-s", "--start":
			//StartTime *int64
			v := args[idx+1]
			i, e := strconv.Atoi(v)
			if e == nil {
				inputs.StartTime = aws.Int64(int64(i))
				continue
			}
			ii, e := time.Parse(shortForm, v)
			if e == nil {
				inputs.StartTime = aws.Int64(int64(ii.Unix()))
				continue
			}
			iii, e := time.Parse(time.RFC3339, v)
			if e == nil {
				inputs.StartTime = aws.Int64(int64(iii.Unix()))
				continue
			}
			panic(fmt.Sprintf("unrecognized StartTime -> %+v", v))
		case "-e", "--end":
			//EndTime *int64
			v := args[idx+1]
			i, e := strconv.Atoi(v)
			if e == nil {
				inputs.EndTime = aws.Int64(int64(i))
				continue
			}
			ii, e := time.Parse(shortForm, v)
			if e == nil {
				inputs.EndTime = aws.Int64(int64(ii.Unix()))
				continue
			}
			iii, e := time.Parse(time.RFC3339, v)
			if e == nil {
				inputs.EndTime = aws.Int64(int64(iii.Unix()))
				continue
			}
			panic(fmt.Sprintf("unrecognized EndTime -> %+v", v))
		case "-l", "--limit":
			//Limit *int32
			v := args[idx+1]
			i, e := strconv.Atoi(v)
			if e != nil {
				panic(fmt.Sprintf("unrecognized Limit -> %+v", v))
			}
			inputs.Limit = aws.Int32(int32(i))
		case "-i", "--identifiers":
			//LogGroupIdentifiers []string
			v := strings.Split(args[idx+1], ",")
			inputs.LogGroupIdentifiers = v
		case "-n", "--name":
			//LogGroupName *string
			v := args[idx+1]
			inputs.LogGroupName = &v
		case "-N", "--names":
			//LogGroupNames []string
			v := strings.Split(args[idx+1], ",")
			inputs.LogGroupNames = v
		default:
			panic(fmt.Sprintf("unrecognized flag %s", args[idx]))
		}
	}
	return inputs
}

// End Parameters

// CloudWatchAPI
// FROM: https://pkg.go.dev/context
// Do not store Contexts inside a struct type; instead, pass a Context explicitly to each function that needs it.
// The Context should be the first parameter, typically named ctx:
type CloudWatchAPI struct {
	client  cloudwatchlogs.Client
	logger  Logging
	storage *Storage // NOTE: can be interfaced
}

func NewApi() CloudWatchAPI {
	if AwsConfig == nil {
		panic("aws config is not loaded correctly. Check ~/.aws")
	}
	return CloudWatchAPI{client: *cloudwatchlogs.NewFromConfig(*AwsConfig)}
}

func (api *CloudWatchAPI) WithLogger(logger Logging) {
	api.logger = logger
}

func (api *CloudWatchAPI) WithStorage(storage *Storage) {
	api.storage = storage
}

func (api *CloudWatchAPI) GetQueryId(ctx context.Context) string {
	str := ctx.Value(QUERYID).(*string)
	if str != nil {
		return *str
	} else {
		return ""
	}
}

func (api *CloudWatchAPI) Exec(ctx context.Context, input *CloudWatchParams) CWResult {
	err := api.Start(ctx, input)
	if err != nil {
		return CWResult{err: err}
	}
	res, err := api.Run(input, ctx, time.Minute*5)
	if err != nil {
		return CWResult{err: err}
	}
	converted := convert(*res)
	jsonb, err := json.Marshal(converted)
	if err != nil {
		api.logger.Error(fmt.Sprintf("Failed to encode result into json due to %s, queryid=%s", err, *input.queryId))
	}

	if api.storage != nil {
		err = api.storage.Insert("insert into results (queryid, result) values ($queryid, $result);", queryParams{
			"$queryid": *input.queryId,
			"$result":  jsonb,
		})

		if err != nil {
			api.logger.Warning(err.Error())
		} else {
			api.logger.Debug(fmt.Sprintf("inserted result for %s", *input.queryId))
		}
	}

	result := CWResult{params: input, result: &converted, json: jsonb, err: err}
	return result
}

func (api *CloudWatchAPI) Start(ctx context.Context, input *CloudWatchParams) error {
	api.logger.Debug(fmt.Sprintf("start query %s", *input.queryInput.QueryString))
	queryOutput, err := api.client.StartQuery(ctx, &input.queryInput)
	if err != nil {
		api.logger.Error(fmt.Sprintf("Failed to start CloudWatch query with err=%s", err.Error()))
		return err
	}
	queryId := queryOutput.QueryId
	input.AssignId(queryId)
	api.logger.Debug(fmt.Sprintf("create queryId=%s, start=%d, end=%d", *queryOutput.QueryId, *input.queryInput.StartTime, *input.queryInput.EndTime))

	if api.storage != nil {
		var group string
		if input.queryInput.LogGroupName != nil {
			group = *input.queryInput.LogGroupName
		} else {
			group = strings.Join(input.queryInput.LogGroupNames, ",")
		}
		insertParams := queryParams{
			":id": *queryId,
			":q":  *(input.queryInput.QueryString),
			":g":  group,
		}
		api.logger.Debug(fmt.Sprintf("insert %s", *input.queryId))
		_ = api.storage.Insert(`insert into queries (query, groups, queryid) values (:q, :g, :id)`, insertParams)
	}
	return nil
}

func (api *CloudWatchAPI) iterate(ctx context.Context) types.QueryStatus {
	queryId := api.GetQueryId(ctx)

	output, err := api.client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryId})
	if err != nil {
		// FIXME: check api.logger is not nil
		api.logger.Error(fmt.Sprintf("Failed GetQueryResults=%s, queryid=%s", err.Error(), queryId))
	}
	return output.Status
}

// CloudWatch state machine
func (api *CloudWatchAPI) Run(params *CloudWatchParams, ctx context.Context, timeout time.Duration) (*[][]types.ResultField, error) {
	var err error = nil
	var counter int64 = 0
	var retry int = 0
	var result *[][]types.ResultField = nil

	client := &api.client
	logger := api.logger
	queryId := params.queryId
	timeInput := params.timeInput
	ctx = context.WithValue(ctx, QUERYID, queryId)
	ctxWT, cancelFunc := context.WithTimeout(ctx, timeout)
	defer cancelFunc()
	logger.Info(fmt.Sprintf("exec id=%s, query=%s, start=%d, end=%d", *params.queryId, *params.queryInput.QueryString, *params.queryInput.StartTime, *params.queryInput.EndTime))

	for {
		switch status := api.iterate(ctxWT); status {
		case types.QueryStatusScheduled:
			time.Sleep(100 * time.Millisecond)
			logger.Debug("Scheduled")
		case types.QueryStatusRunning:
			var duration int64
			if counter*100 < 2000 {
				duration = counter * 100
				counter += 1
			} else {
				duration = 2000
			}
			sleep := time.Duration(duration) * time.Millisecond
			if timeInput.max != nil {
				maxDuration := time.Duration(*timeInput.max) * time.Second
				if sleep > maxDuration {
					sleep = maxDuration
				}
			}
			time.Sleep(sleep)
			api.logger.Debug("Running")
		case types.QueryStatusComplete:
			api.logger.Debug("Complete")
			output, errGetResult := client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: queryId})
			if errGetResult != nil {
				err = errGetResult
			}
			result = &output.Results
			api.logger.Debug(fmt.Sprintf("Got Result, id=%s, len=%d", *params.queryId, len(*result)))
			return result, err
		case types.QueryStatusFailed:
			logger.Debug("Query Failed")
			if retry > MAX_RETRY {
				logger.Info("reached maximum retry for status failed. exit loop")
				err = errors.New(string(status))
				return result, nil
			} else {
				retry += 1
			}
		case types.QueryStatusCancelled:
			logger.Debug("Query Cancelled")
			err = errors.New(string(status))
			return result, err
		case types.QueryStatusTimeout:
			logger.Debug("Query Timeout")
			err = errors.New(string(status))
			return result, err
		case types.QueryStatusUnknown:
			logger.Debug("Query Unknown status")
			if retry > MAX_RETRY {
				logger.Info("reached maximum retry for status unknown. exit loop")
				err = errors.New(string(status))
				return result, err
			} else {
				retry += 1
			}
		default:
			panic(status)
		}
	}
}

type CloudWatchResult = []map[string]string

type CWResult struct {
	params *CloudWatchParams
	result *CloudWatchResult
	json   []byte
	err    error
}

func (r *CWResult) IsOk() bool {
	return r.result != nil
}

// End CloudWatchAPI

func convert(results [][]types.ResultField) CloudWatchResult {

	var arrays = make([]map[string]string, len(results))

	for idx, fields := range results {
		var new = map[string]string{}
		for _, field := range fields {
			new[*field.Field] = *field.Value
		}
		arrays[idx] = new
	}
	return arrays
}

// Storage
type Storage struct {
	dbpool   *sqlitex.Pool
	ctx      context.Context
	dbmutext *sync.Mutex
}

func newStorage() Storage {
	dbpool, err := sqlitex.NewPool("file:data.db", sqlitex.PoolOptions{})
	if err != nil {
		// panic("failed to create new DB pool", err)
		return Storage{dbpool: nil, ctx: nil}
	}
	ctx := context.TODO()
	mu := sync.Mutex{}
	s := Storage{dbpool: dbpool, ctx: ctx, dbmutext: &mu}
	conn := s.newConn()
	defer s.closeConn(conn)

	_ = s.Execute(`CREATE table IF NOT EXISTS queries(id INTEGER PRIMARY KEY AUTOINCREMENT, time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, query TEXT, groups TEXT, queryid TEXT DEFAULT null);`)
	_ = s.Execute(`CREATE UNIQUE INDEX IF NOT EXISTS queries_index ON queries(id, time);`)

	_ = s.Execute(`CREATE table IF NOT EXISTS results(queryid TEXT, result BLOB, FOREIGN KEY (queryid) REFERENCES queries(queryid));`)
	return s
}

func (s *Storage) newConn() *sqlite.Conn {
	conn, err := s.dbpool.Take(s.ctx)
	if err != nil {
		// FIXME: warning
		return nil
	}
	return conn
}

func (s *Storage) closeConn(conn *sqlite.Conn) {
	if conn != nil {
		s.dbpool.Put(conn)
	}
}

func (s *Storage) Execute(query string) error {
	conn := s.newConn()
	defer s.closeConn(conn)
	if conn == nil {
		return nil
	}
	stmt := conn.Prep(query)
	_, err := stmt.Step()
	return err
}

type queryParams map[string]interface{}

func (s *Storage) Insert(query string, params queryParams) error {
	conn := s.newConn()
	defer s.closeConn(conn)
	if conn == nil {
		return nil
	}
	stmt := conn.Prep(query)

	for key, value := range params {
		switch value.(type) {
		case string:
			stmt.SetText(key, value.(string))
		case []byte:
			stmt.SetBytes(key, value.([]byte))
		}
	}

	_, err := stmt.Step()
	return err
}

// End Storage

func init() {
	cfg, _ := load()
	AwsConfig = &cfg
	level, exist := os.LookupEnv("LogLevel")
	if !exist {
		level = "info"
	} else {
		level = strings.ToLower(level)
	}
	zerologlevel := EnvLogLevel[level]
	LogLevel = &zerologlevel
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(*LogLevel)
	logger := log.Logger

	logger.Info().Msg(fmt.Sprintf("main"))
	storage := newStorage()

	args := os.Args[1:]

	var bounds []Bound
	params := parseParams(args)
	logger.Info().Msg(fmt.Sprintf("query=%s", *params.queryInput.QueryString))

	ctx := context.Background()
	ctx = logger.WithContext(ctx)
	bounds = findBounds(ctx, params)
	log.Info().Msg(fmt.Sprintf("search len=%d,bounds=%+v", len(bounds), bounds))

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	resultChan := make(chan CWResult, 4)
	sem := make(chan struct{}, 4)
	cancelFuncs := make([]func(), len(bounds))
	wg := sync.WaitGroup{}

	f := func(wg *sync.WaitGroup, ctx context.Context) <-chan CWResult {
		for idx, bound := range bounds {
			sem <- struct{}{}
			logger.Info().Msg(fmt.Sprintf("send bound=%+v", bound))
			wg.Add(1)
			go func(param CloudWatchParams, b Bound, wg *sync.WaitGroup, ctx context.Context, sem <-chan struct{}, resultChan chan<- CWResult) {
				defer wg.Done()
				p := param
				bound := b
				p.Assign(bound)
				ctxC, cancelFunc := context.WithCancel(ctx)
				cancelFuncs[idx] = cancelFunc
				cloudapi := NewApi()
				cloudapi.WithLogger(Logger{zerolog.Ctx(ctx)})
				cloudapi.WithStorage(&storage)

				res := cloudapi.Exec(ctxC, &p)
				if res.result != nil {
					resultChan <- res
				}
				<-sem
			}(params, bound, wg, ctx, sem, resultChan)
		}
		close(sem)
		return resultChan
	}
	ResultChan := f(&wg, ctx)

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	counter := 0
Exit:
	for {
		select {
		case res := <-ResultChan:
			fmt.Println(fmt.Sprintf("%s, CWResultJson=%s", *res.params.queryId, res.json))
			fmt.Println(fmt.Sprintf("Done %d", counter))
			counter += 1
			if counter >= len(bounds) {
				break Exit
			}
		case <-sigChan:
			logger.Println("Cancelled")
			for _, cancelF := range cancelFuncs {
				cancelF()
			}
			break Exit
		}
	}
	fmt.Println(fmt.Sprintf("end main"))

	os.Exit(0)
}

type Bound struct {
	start int64
	end   int64
}

func NewBoundFromParams(params CloudWatchParams) Bound {
	b := Bound{*params.queryInput.StartTime, *params.queryInput.EndTime}
	return b
}

func (b Bound) split() (Bound, Bound) {
	start := time.Unix(b.start, 0)
	end := time.Unix(b.end, 0)
	dur := time.Duration(float64(end.Sub(start))/2/math.Pow10(9)) * time.Second
	s2 := start.Add(dur)
	return Bound{start.Unix(), s2.Unix()}, Bound{s2.Unix(), end.Unix()}
}

func searchBound(b Bound, params CloudWatchParams, cloudapi CloudWatchAPI, arr []Bound) []Bound {
	ctx := context.TODO()
	params.Assign(b)
	res := cloudapi.Exec(ctx, &params)
	if res.err != nil {
		panic(fmt.Sprintf("FIXME: to implement to address: %s", res.err.Error()))
	}
	v, _ := strconv.Atoi((*res.result)[0]["count()"])
	if v <= 10000 {
		cloudapi.logger.Debug("proceeding since less than 10000")
		arr = append(arr, b)
		cloudapi.logger.Debug(fmt.Sprintf("done val=%d, id=%s, start=%d, end=%d", v, *params.queryId, *params.queryInput.StartTime, *params.queryInput.EndTime))
	} else if params.fetchAllLengthyQuery {
		cloudapi.logger.Debug("split")
		dur1, dur2 := b.split()
		cloudapi.logger.Debug(fmt.Sprintf("val=%d, split1:(start=%d, end=%d), split2:(start=%d, end=%d)", v, dur1.start, dur1.end, dur2.start, dur2.end))
		arr = searchBound(dur1, params, cloudapi, arr)
		arr = searchBound(dur2, params, cloudapi, arr)
	} else {
		cloudapi.logger.Debug("skipping")
		arr = append(arr, b)
		cloudapi.logger.Debug(fmt.Sprintf("done val=%d, id=%s, start=%d, end=%d", v, *params.queryId, *params.queryInput.StartTime, *params.queryInput.EndTime))
	}
	return arr
}

func search(b Bound, params CloudWatchParams, cloudapi CloudWatchAPI) []Bound {
	arr := make([]Bound, 1)
	arr = searchBound(b, params, cloudapi, arr)[1:]
	return arr
}

func findBounds(ctx context.Context, params CloudWatchParams) []Bound {
	// Since CloudWatch limits the output to 10000
	// this function devides the start / end time by half
	// and sequencially finds a pair (start, end)
	// in which a total count is less than 10000.
	cloudapi := NewApi()
	cloudapi.WithLogger(Logger{zerolog.Ctx(ctx)})
	cloudapi.logger.Debug("find bounds")

	query := Query(*params.queryInput.QueryString)
	var newQuery string
	if !query.check() {
		newQuery = query.add()
	} else {
		newQuery = string(query)
	}
	params.queryInput.QueryString = aws.String(newQuery)
	cloudapi.logger.Debug(newQuery)

	b := Bound{*params.queryInput.StartTime, *params.queryInput.EndTime}
	chs := search(b, params, cloudapi)
	cloudapi.logger.Debug("Done finding bounds")
	return chs
}
