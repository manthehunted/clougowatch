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

// parameters
type TimeParam struct {
	count int
	max   *int
}

type CloudWatchParams struct {
	queryInput cloudwatchlogs.StartQueryInput
	timeInput  *TimeParam
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
	client cloudwatchlogs.Client
	logger *log.Logger
	params *CloudWatchParams
}

func (api *CloudWatchAPI) GetQueryId(ctx context.Context) string {
	str := ctx.Value(QUERYID).(*string)
	if str != nil {
		return *str
	} else {
		return ""
	}
}

func (api *CloudWatchAPI) Start(ctx context.Context, input *CloudWatchParams) *string {
	api.params = input
	api.logger.Println(fmt.Sprintf("start query %s", *input.queryInput.QueryString))
	queryOutput, err := api.client.StartQuery(ctx, &input.queryInput)
	if err != nil {
		panic(fmt.Sprintf("Failed to start CloudWatch query with err=%s", err.Error()))
	}

	return queryOutput.QueryId
}

func (api *CloudWatchAPI) iterate(ctx context.Context) types.QueryStatus {
	queryId := api.GetQueryId(ctx)

	output, err := api.client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryId})
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed GetQueryResults=%s, queryid=%s", err.Error(), queryId))
	}
	return output.Status
}

// CloudWatch state machine
func (api *CloudWatchAPI) Run(queryId string, ctx context.Context, timeout time.Duration) (*[][]types.ResultField, error) {
	var err error = nil
	var counter int64 = 0
	var retry int = 0
	var result *[][]types.ResultField = nil

	client := &api.client
	logger := api.logger
	timeInput := api.params.timeInput
	ctx = context.WithValue(ctx, QUERYID, &queryId)
	ctxWT, cancelFunc := context.WithTimeout(ctx, timeout)
	defer cancelFunc()

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
			output, errGetResult := client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryId})
			if errGetResult != nil {
				err = errGetResult
			}
			result = &output.Results
			api.logger.Println("Got Result")
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

func convert(results [][]types.ResultField) []map[string]string {

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

type queryParams map[string]interface{}
type storage struct {
	dbpool *sqlitex.Pool
	ctx    context.Context
}

func newStorage() storage {
	dbpool, err := sqlitex.NewPool("file:data.db", sqlitex.PoolOptions{})
	if err != nil {
		// panic("failed to create new DB pool", err)
		return storage{nil, nil}
	}
	ctx := context.TODO()
	s := storage{dbpool, ctx}
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

func (s *storage) newConn() *sqlite.Conn {
	conn, err := s.dbpool.Take(s.ctx)
	if err != nil {
		// FIXME: warning
		return nil
	}
	return conn
}

func (s *storage) closeConn(conn *sqlite.Conn) {
	if conn != nil {
		s.dbpool.Put(conn)
	}
}

func (s *storage) Execute(query string) error {
	conn := s.newConn()
	defer s.closeConn(conn)
	if conn == nil {
		return nil
	}
	stmt := conn.Prep(query)
	_, err := stmt.Step()
	return err
}

func (s *storage) Insert(query string, params queryParams) error {
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
	result *[][]types.ResultField
	err    error
}

func (r *CWResult) IsOk() bool {
	return r.result != nil
}

var getAWSConfig = sync.OnceValue(func() aws.Config { cfg, _ := load(); return cfg })

func _main() {
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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	resultChan := make(chan CWResult, 1)

	var result CWResult
	var ctx context.Context
	var group string

	wg := sync.WaitGroup{}
	ctx = context.TODO()
	ctxC, cancelF := context.WithCancel(ctx)
	defer cancelF()

	cfg := getAWSConfig()
	cloudApi := CloudWatchAPI{client: *cloudwatchlogs.NewFromConfig(cfg), logger: logger}

	params := parseParams(args)
	queryId := cloudApi.Start(ctxC, &params)
	logger.Println(fmt.Sprintf("queryId: %s", *queryId))

	if cloudApi.params.queryInput.LogGroupName != nil {
		group = *cloudApi.params.queryInput.LogGroupName
	} else {
		group = strings.Join(cloudApi.params.queryInput.LogGroupNames, ",")
	}
	insertParams := queryParams{
		":id": *queryId,
		":q":  *(cloudApi.params.queryInput.QueryString),
		":g":  group,
	}
	_ = storage.Insert(`insert into queries (query, groups, queryid) values (:q, :g, :id)`, insertParams)

	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := cloudApi.Run(*queryId, ctxC, time.Minute*5)
		resultChan <- CWResult{result: res, err: err}
	}()

	select {
	case result = <-resultChan:
		if result.IsOk() {
			logger.Println("Done")
		} else {

			logger.Fatalln(fmt.Sprintf("no result for queryid=%s due to %s", *queryId, result.err))
		}
	case <-sigChan:
		logger.Println("Cancelled")
		cancelF()
	}
	wg.Wait()

	jsonb, err := json.Marshal(convert(*result.result))
	if err != nil {
		logger.Fatalln(fmt.Sprintf("Failed to encode result into json due to %s, queryid=%s", err, *queryId))
	}

	err = storage.Insert("insert into results (queryid, result) values ($queryid, $result);", queryParams{
		"$queryid": *queryId,
		"$result":  jsonb,
	})

	cmd := exec.Command("python", "../py/compare.py", *queryId)
	out, err := cmd.Output()
	fmt.Println(fmt.Sprintf("%s", out))
	fmt.Println(fmt.Sprintf("%s", err))

	os.Exit(0)
}

type Bound struct {
	start int64
	end   int64
}

func (b *Bound) split() (Bound, Bound) {
	start := time.Unix(b.start, 0)
	end := time.Unix(b.end, 0)
	dur := time.Duration(float64(end.Sub(start))/2/math.Pow10(9)) * time.Second
	s2 := start.Add(dur)
	return Bound{start.Unix(), s2.Unix()}, Bound{s2.Unix(), end.Unix()}
}

func make2(b Bound, params CloudWatchParams, cfg aws.Config) {
	writer := os.Stdout
	logger := log.New(writer, "logger: ", log.Lshortfile|log.Ldate|log.Ltime)
	cloudApi := CloudWatchAPI{client: *cloudwatchlogs.NewFromConfig(cfg), logger: logger}
	params.Assign(b)

	ctx := context.TODO()
	queryId := cloudApi.Start(ctx, &params)
	res, _ := cloudApi.Run(*queryId, ctx, time.Minute*5)
	v, _ := strconv.Atoi(convert(*res)[0]["count()"])
	if v <= 10000 {
		logger.Println(fmt.Sprintf("done val=%d, id=%s, start=%d, end=%d", v, *queryId, *params.queryInput.StartTime, *params.queryInput.EndTime))
		return
	}

	dur1, dur2 := b.split()
	logger.Println(fmt.Sprintf("split1:(start=%d, end=%d), split2:(start=%d, end=%d)", dur1.start, dur1.end, dur2.start, dur2.end))
	make2(dur1, params, cfg)
	make2(dur2, params, cfg)
}

func main() {
	writer := os.Stdout
	logger := log.New(writer, "logger: ", log.Lshortfile|log.Ldate|log.Ltime)
	cfg := getAWSConfig()
	args := os.Args[2:]
	params := parseParams(args)
	b := Bound{*params.queryInput.StartTime, *params.queryInput.EndTime}
	cloudApi := CloudWatchAPI{client: *cloudwatchlogs.NewFromConfig(cfg), logger: logger}
	chs := search(b, params, cloudApi)
	for _, k := range chs {
		cloudApi.logger.Println(fmt.Sprintf("found bound: %+v", k))
	}
	logger.Println("done main")
}
