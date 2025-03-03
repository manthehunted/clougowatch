package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"log"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const shortForm = "2006-01-02"
const MAX_RETRY = 3
const QUERYID = "queryId"

var once sync.Once

func load() (aws.Config, *string) {
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	client := sts.NewFromConfig(cfg)
	input := &sts.GetCallerIdentityInput{}
	result, err := client.GetCallerIdentity(context.TODO(), input)
	if err != nil {
		log.Fatal(err)
	}
	return cfg, result.Account
}

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

// parameters
type TimeParam struct {
	count int
	max   *int
}

type CloudWatchParams struct {
	queryInput cloudwatchlogs.StartQueryInput
	timeInput  *TimeParam
	queryId    *string
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

func parseParams(args []string) CloudWatchParams {
	queryInput := parseCloudParams(args)
	timeParams := parseTimeParams(args)
	return CloudWatchParams{queryInput: queryInput, timeInput: &timeParams}
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
		}
	}
	return inputs
}

// FROM: https://pkg.go.dev/context
// Do not store Contexts inside a struct type; instead, pass a Context explicitly to each function that needs it.
// The Context should be the first parameter, typically named ctx:
type CloudWatchAPI struct {
	client  cloudwatchlogs.Client
	logger  *log.Logger
	storage *Storage // NOTE: can be interfaced
}

func NewApi() CloudWatchAPI {
	cfg := getAWSConfig()
	return CloudWatchAPI{client: *cloudwatchlogs.NewFromConfig(cfg)}
}

func (api *CloudWatchAPI) WithLogger(logger *log.Logger) {
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

func (api *CloudWatchAPI) Exec(ctx context.Context, input *CloudWatchParams) (CloudWatchResult, error) {
	err := api.Start(ctx, input)
	if err != nil {
		return make(CloudWatchResult, 0), err
	}
	res, err := api.Run(input, ctx, time.Minute*5)
	if err != nil {
		return make(CloudWatchResult, 0), err
	}
	result := convert(*res)

	if api.storage != nil {
		jsonb, err := json.Marshal(result) // FIXME: take care json encoding err
		if err != nil {
			api.logger.Println(fmt.Sprintf("Failed to encode result into json due to %s, queryid=%s", err, *input.queryId))
		} else {
			err = api.storage.Insert("insert into results (queryid, result) values ($queryid, $result);", queryParams{
				"$queryid": *input.queryId,
				"$result":  jsonb,
			})
			if err != nil {
				api.logger.Println(err) // FIXME: put warning
			} else {
				api.logger.Println(fmt.Sprintf("inserted result for %s", *input.queryId))
			}
		}
	}
	return result, nil
}

func (api *CloudWatchAPI) Start(ctx context.Context, input *CloudWatchParams) error {
	api.logger.Println(fmt.Sprintf("start query %s", *input.queryInput.QueryString))
	queryOutput, err := api.client.StartQuery(ctx, &input.queryInput)
	if err != nil {
		api.logger.Println(fmt.Sprintf("Failed to start CloudWatch query with err=%s", err.Error()))
		return err
	}
	queryId := queryOutput.QueryId
	input.AssignId(queryId)
	api.logger.Println(fmt.Sprintf("create queryId=%s, start=%d, end=%d", *queryOutput.QueryId, *input.queryInput.StartTime, *input.queryInput.EndTime))

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
		_ = api.storage.Insert(`insert into queries (query, groups, queryid) values (:q, :g, :id)`, insertParams)
	}
	return nil
}

func (api *CloudWatchAPI) iterate(ctx context.Context) types.QueryStatus {
	queryId := api.GetQueryId(ctx)

	output, err := api.client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryId})
	if err != nil {
		api.logger.Fatalln(fmt.Sprintf("Failed GetQueryResults=%s, queryid=%s", err.Error(), queryId))
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
	logger.Println(fmt.Sprintf("exec id=%s, query=%s, start=%d, end=%d", *params.queryId, *params.queryInput.QueryString, *params.queryInput.StartTime, *params.queryInput.EndTime))

	for {
		switch status := api.iterate(ctxWT); status {
		case types.QueryStatusScheduled:
			time.Sleep(100 * time.Millisecond)
			logger.Println("Scheduled")
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
			api.logger.Println("Running")
		case types.QueryStatusComplete:
			api.logger.Println("Complete")
			output, errGetResult := client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: queryId})
			if errGetResult != nil {
				err = errGetResult
			}
			result = &output.Results
			api.logger.Println(fmt.Sprintf("Got Result, id=%s, len=%d", *params.queryId, len(*result)))
			return result, err
		case types.QueryStatusFailed:
			logger.Println("Query Failed")
			if retry > MAX_RETRY {
				logger.Println("reached maximum retry for status failed. exit loop")
				err = errors.New(string(status))
				return result, nil
			} else {
				retry += 1
			}
		case types.QueryStatusCancelled:
			logger.Println("Query Cancelled")
			err = errors.New(string(status))
			return result, err
		case types.QueryStatusTimeout:
			logger.Println("Query Timeout")
			err = errors.New(string(status))
			return result, err
		case types.QueryStatusUnknown:
			logger.Println("Query Unknown status")
			if retry > MAX_RETRY {
				logger.Println("reached maximum retry for status unknown. exit loop")
				err = errors.New(string(status))
				return result, err
			} else {
				retry += 1
			}
		}
	}
}

type CloudWatchResult = []map[string]string

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

type Storage struct {
	dbpool *sqlitex.Pool
	ctx    context.Context
}

func newStorage() Storage {
	dbpool, err := sqlitex.NewPool("file:data.db", sqlitex.PoolOptions{})
	if err != nil {
		// panic("failed to create new DB pool", err)
		return Storage{nil, nil}
	}
	ctx := context.TODO()
	s := Storage{dbpool, ctx}
	conn := s.newConn()
	defer s.closeConn(conn)

	_ = s.Execute(`CREATE table IF NOT EXISTS queries(
		time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		query TEXT,
		groups TEXT,
		queryid TEXT DEFAULT null,
		primary key (time)
	);`)

	_ = s.Execute("CREATE table IF NOT EXISTS results(id INTEGER PRIMARY KEY autoincrement, queryid TEXT, result BLOB);")
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

type CWResult struct {
	result *CloudWatchResult
	err    error
}

func (r *CWResult) IsOk() bool {
	return r.result != nil
}

var getAWSConfig = sync.OnceValue(func() aws.Config { cfg, _ := load(); return cfg })

func main() {
	writer := os.Stdout
	defer writer.Close()

	logger := log.New(writer, "logger: ", log.Lshortfile|log.Ldate|log.Ltime)

	storage := newStorage()

	task, args := os.Args[1], os.Args[2:]
	// TODO: refactor to handler style
	switch task {
	case "db":
		cmd := args[0]

		// FIXME: cleanup
		logger.Println(cmd)
		conn := storage.newConn()
		defer storage.closeConn(conn)
		stmt := conn.Prep(fmt.Sprintf("%s", cmd))
		data := make([][]string, 0)
		ncols := stmt.ColumnCount()
		nrow := 0

		for {
			hasRow, err := stmt.Step()
			if err != nil {
				logger.Println(fmt.Sprintf("while step DB with %s", err.Error()))
			}

			if !(hasRow) && (nrow == 0) {
				logger.Fatalln("unexpectedly no data. Check DB entry or query")
			} else if !hasRow {
				break
			}
			if nrow == 0 {
				columns := make([]string, ncols)
				for i := 0; i <= ncols; i++ {
					columns = append(columns, stmt.ColumnName(i))
				}
				data = append(data, columns)
				nrow += 1
			}

			row := make([]string, ncols)
			for i := 0; i <= ncols; i++ {
				row = append(row, stmt.ColumnText(i))
			}
			data = append(data, row)
		}

		for idx, row := range data {
			if idx > 0 {
				os.Stdout.Write([]byte(fmt.Sprintf("%s", row)))
			}
		}

		os.Exit(0)
	case "query":
		logger.Println("query")
	case "longquery":
		logger.Println("longquery")
	default:
		logger.Panic(fmt.Sprintf("unrecognizable cmd=%s", task))
	}

	var bounds []Bound
	params := parseParams(args)
	if task == "query" {
		bounds = make([]Bound, 1)
		bounds = append(bounds, NewBoundFromParams(params))
	} else {
		bounds = findBounds(os.Args[2:], logger)
		logger.Println(fmt.Sprintf("search len=%d,bounds=%+v", len(bounds), bounds))
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	resultChan := make(chan CWResult, 4)
	sem := make(chan struct{}, 4)
	// done := make(chan bool, 1)

	wg := sync.WaitGroup{}
	ctx := context.TODO()

	f := func(wg *sync.WaitGroup, ctx *context.Context) <-chan CWResult {
		for _, bound := range bounds {
			sem <- struct{}{}
			logger.Println(fmt.Sprintf("send bound=%+v", bound))
			wg.Add(1)
			go func(param CloudWatchParams, b Bound, wg *sync.WaitGroup, ctx *context.Context, sem <-chan struct{}, resultChan chan<- CWResult) {
				defer wg.Done()
				// defer cancelF()
				p := param
				bound := b
				p.Assign(bound)
				// FIXME:
				ctxC, _ := context.WithCancel(*ctx)
				cloudapi := NewApi()
				cloudapi.WithLogger(logger)
				cloudapi.WithStorage(&storage)

				res, err := cloudapi.Exec(ctxC, &p)
				resultChan <- CWResult{result: &res, err: err}
				<-sem
			}(params, bound, wg, ctx, sem, resultChan)
		}
		logger.Println("closing")
		close(sem)
		return resultChan
	}
	r := f(&wg, &ctx)

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	c := 0
	for range r {
		fmt.Println(fmt.Sprintf("Done %d", c))
		c += 1
	}
	fmt.Println(fmt.Sprintf("end main"))

	// python call
	cmd := exec.Command("python", "../py/compare.py", *params.queryId)
	out, err := cmd.Output()
	fmt.Println(fmt.Sprintf("%s", out))
	fmt.Println(fmt.Sprintf("%s", err))

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
	res, err := cloudapi.Exec(ctx, &params)
	if err != nil {
		// FIXME:
	}
	v, _ := strconv.Atoi(res[0]["count()"])
	if v <= 10000 {
		arr = append(arr, b)
		cloudapi.logger.Println(fmt.Sprintf("done val=%d, id=%s, start=%d, end=%d", v, *params.queryId, *params.queryInput.StartTime, *params.queryInput.EndTime))
	} else {
		dur1, dur2 := b.split()
		cloudapi.logger.Println(fmt.Sprintf("val=%d, split1:(start=%d, end=%d), split2:(start=%d, end=%d)", v, dur1.start, dur1.end, dur2.start, dur2.end))
		arr = searchBound(dur1, params, cloudapi, arr)
		arr = searchBound(dur2, params, cloudapi, arr)
	}
	return arr
}

func search(b Bound, params CloudWatchParams, cloudapi CloudWatchAPI) []Bound {
	arr := make([]Bound, 1)
	arr = searchBound(b, params, cloudapi, arr)[1:]
	return arr
}

func findBounds(args []string, logger *log.Logger) []Bound {
	// Since CloudWatch limits the output to 10000
	// this function devides the start / end time by half
	// and sequencially finds a pair (start, end)
	// in which a total count is less than 10000.
	cloudapi := NewApi()
	cloudapi.WithLogger(logger)
	params := parseParams(args)

	query := Query(*params.queryInput.QueryString)
	var newQuery string
	if !query.check() {
		newQuery = query.add()
	} else if strings.Contains(strings.ReplaceAll(string(query), " ", ""), "|pattern") {
		panic("not implemented")
	} else {
		newQuery = string(query)
	}
	params.queryInput.QueryString = aws.String(newQuery)
	logger.Println(newQuery)

	b := Bound{*params.queryInput.StartTime, *params.queryInput.EndTime}
	chs := search(b, params, cloudapi)
	return chs
}
