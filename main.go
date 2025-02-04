package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"log"
	"os"
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
			v := args[idx+1]
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

type CloudWatchAPI struct {
	client cloudwatchlogs.Client
	ctx    context.Context
	logger *log.Logger
	params *CloudWatchParams
}

func FromConfig(cfg aws.Config, logger *log.Logger) CloudWatchAPI {
	client := cloudwatchlogs.NewFromConfig(cfg)
	return CloudWatchAPI{client: *client, ctx: context.Background(), logger: logger}
}

func (api *CloudWatchAPI) GetQueryId() string {
	str := api.ctx.Value(QUERYID).(*string)
	if str != nil {
		return *str
	} else {
		return ""
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

func (api *CloudWatchAPI) Start(input *CloudWatchParams) {
	api.params = input
	queryOutput, err := api.client.StartQuery(api.ctx, &input.queryInput)
	if err != nil {
		panic(fmt.Sprintf("Failed to start CloudWatch query with err=%s", err.Error()))
	}

	api.ctx = context.WithValue(api.ctx, QUERYID, queryOutput.QueryId)
}

func (api *CloudWatchAPI) iterate() types.QueryStatus {
	queryId := api.GetQueryId()

	output, err := api.client.GetQueryResults(api.ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryId})
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed GetQueryResults=%s, queryid=%s", err.Error(), queryId))
	}
	return output.Status
}

// CloudWatch state machine
func (api *CloudWatchAPI) Run() [][]types.ResultField {
	var counter int64 = 0
	var retry int = 0
	var result [][]types.ResultField

	client := &api.client
	ctx := api.ctx
	logger := api.logger
	queryId := api.GetQueryId()
	timeInput := api.params.timeInput

done:
	for {
		switch status := api.iterate(); status {
		case types.QueryStatusScheduled:
			time.Sleep(500 * time.Millisecond)
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
			output, err := client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: &queryId})
			if err != nil {
				logger.Fatalln(fmt.Sprintf("Failed to get results: %s", err.Error()))
			}
			result = output.Results
			break done
		case types.QueryStatusFailed:
			logger.Println("Query Failed")
			if retry > MAX_RETRY {
				logger.Println("reached maximum retry for status failed. exit loop")
				break done
			} else {
				retry += 1
			}
		case types.QueryStatusCancelled:
			logger.Println("Query Cancelled")
			break done
		case types.QueryStatusTimeout:
			logger.Println("Query Timeout")
			break done
		case types.QueryStatusUnknown:
			logger.Println("Query Unknown status")
			if retry > MAX_RETRY {
				logger.Println("reached maximum retry for status unknown. exit loop")
				break done
			} else {
				retry += 1
			}
		}
	}

	return result
}

func (api *CloudWatchAPI) Write() bool {
	return true
}

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
	// conn, err := dbpool.Take(ctx)
	return storage{dbpool, ctx}
}

func (s *storage) newConn() *sqlite.Conn {
	conn, err := s.dbpool.Take(s.ctx)
	if err != nil {
		return nil
	}
	return conn
}

func main() {
	f, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		f = os.Stdout
	}
	defer f.Close()

	logger := log.New(f, "logger: ", log.Lshortfile|log.Ldate|log.Ltime)

	storage := newStorage()
	conn := storage.newConn()
	if conn != nil {
		defer storage.dbpool.Put(conn)
	}

	task, args := os.Args[1], os.Args[2:]
	switch task {
	case "query":
		logger.Println("query")
	case "db":
		// FIXME: needs to revisit.
		// code is error prone, but at least work for my purpose.
		var asRaw bool
		var cmd string
		n := len(args)
		switch n {
		case 1:
			asRaw = false
			cmd = args[0]
		case 2:
			if strings.Contains(args[0], "-r") {
				asRaw = true
				cmd = args[1]
			} else {
				cmd = args[0]
				asRaw = strings.Contains(args[1], "-r")
			}
		default:
			panic(fmt.Sprintf("not implemented for %d", len(args)))
		}
		// FIXME: END

		stmt := conn.Prep(fmt.Sprintf("%s", cmd))
		data := make([][]string, 0)
		ncols := stmt.ColumnCount()
		nrow := 0

		for {
			hasRow, err := stmt.Step()
			if err != nil {
				logger.Println(fmt.Sprintf("while step DB with %s", err.Error()))
			}
			if !hasRow {
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

		if asRaw {
			for idx, row := range data {
				if idx > 0 {
					os.Stdout.Write([]byte(fmt.Sprintf("%s", row)))
				}
			}
		} else {
			for _, row := range data {
				os.Stdout.Write([]byte(fmt.Sprintf("%s\n", strings.Join(row, "||"))))
			}
		}

		os.Exit(0)
	default:
		logger.Panic(fmt.Sprintf("unrecognizable cmd=%s", task))
	}

	// create queries
	stmt := conn.Prep(`CREATE table IF NOT EXISTS queries(
		time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		query TEXT,
		groups TEXT,
		queryid TEXT DEFAULT null,
		primary key (time)
	);`)
	_, err = stmt.Step()
	if err != nil {
		logger.Println("failed to create table=queries")
		logger.Println(err)
	}

	// create results
	stmt = conn.Prep("CREATE table IF NOT EXISTS results(id INTEGER PRIMARY KEY autoincrement, queryid TEXT, result BLOB);")
	_, err = stmt.Step()
	if err != nil {
		logger.Println("failed to create table=results")
		logger.Println(err)
	}

	// Create an Amazon S3 service client
	var getConfig = sync.OnceValue(func() aws.Config { cfg, _ := load(); return cfg })
	cfg := getConfig()
	cloudApi := FromConfig(cfg, logger)
	params := parseParams(args)
	cloudApi.Start(&params)

	queryId := cloudApi.GetQueryId()
	stmt = conn.Prep(`insert into queries (query, groups, queryid) values (:q, :g, :id)`)
	stmt.SetText(":id", queryId)
	stmt.SetText(":q", *(cloudApi.params.queryInput.QueryString))
	if cloudApi.params.queryInput.LogGroupName != nil {
		stmt.SetText(":g", *cloudApi.params.queryInput.LogGroupName)
	} else {
		stmt.SetText(":g", strings.Join(cloudApi.params.queryInput.LogGroupNames, ","))
	}
	_, err = stmt.Step()
	if err != nil {
		logger.Println(fmt.Sprintf("Failed to insert data with err=%s", err.Error()))
	}

	results := cloudApi.Run()
	if results == nil {
		logger.Fatalln(fmt.Sprintf("unexpectedly no results for queryid=%s", queryId))
	}

	jsonb, err := json.Marshal(convert(results))
	if err != nil {
		logger.Fatalln(fmt.Sprintf("Failed to encode result into json due to %s, queryid=%s", err.Error(), queryId))
	}

	stmt = conn.Prep("insert into results (queryid, result) values ($queryid, $result);")
	stmt.SetText("$queryid", queryId)
	stmt.SetBytes("$result", jsonb)
	_, err = stmt.Step()
	if err != nil {
		logger.Println(fmt.Sprintf("failed to insert for queryid=%s", queryId))
		logger.Println(err)
	}
	os.Exit(0)
}
