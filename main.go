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

	"zombiezen.com/go/sqlite/sqlitex"
)

const shortForm = "2006-01-02"
const MAX_RETRY = 3

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

type TimeParam struct {
	count int
	max   *int
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

func iterate(client *cloudwatchlogs.Client, ctx context.Context, id string) types.QueryStatus {
	output, err := client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: &id})
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to get CloudWatch results=%s, queryid=%s", err.Error(), id))
	}
	return output.Status
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

func main() {
	f, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		f = os.Stdout
	}
	defer f.Close()

	logger := log.New(f, "logger: ", log.Lshortfile|log.Ldate|log.Ltime)

	dbpool, err := sqlitex.NewPool("file:data.db", sqlitex.PoolOptions{})
	if err != nil {
		logger.Println("failed to create new DB pool")
		panic(err)
	}
	conn, err := dbpool.Take(context.TODO())
	if conn == nil {
		panic("no DB connection established")
	} else if err != nil {
		logger.Println("error from DB")
		panic(err)
	}
	defer dbpool.Put(conn)

	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
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
		logger.Panic(fmt.Sprintf("unrecognizable cmd=%s", cmd))
	}

	// Create an Amazon S3 service client
	var getConfig = sync.OnceValue(func() aws.Config { cfg, _ := load(); return cfg })
	cfg := getConfig()
	client := cloudwatchlogs.NewFromConfig(cfg)
	queryInput := parseCloudParams(args)
	timeParams := parseTimeParams(args)

	// create queries
	stmt := conn.Prep("CREATE table IF NOT EXISTS queries(id INTEGER PRIMARY KEY autoincrement, query TEXT, groups TEXT, time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, queryid TEXT DEFAULT null);")
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

	// start CloudWatch query
	queryOutput, err := client.StartQuery(context.TODO(), &queryInput)
	if err != nil {
		logger.Fatalln(fmt.Sprintf("Failed to start CloudWatch query with err=%s", err.Error()))
	}

	queryid := queryOutput.QueryId

	stmt = conn.Prep(`insert into queries (query, groups, queryid) values (:q, :g, :id)`)
	stmt.SetText(":id", *queryid)
	stmt.SetText(":q", *queryInput.QueryString)
	if queryInput.LogGroupName != nil {
		stmt.SetText(":g", *queryInput.LogGroupName)
	} else {
		stmt.SetText(":g", strings.Join(queryInput.LogGroupNames, ","))
	}
	_, err = stmt.Step()
	if err != nil {
		logger.Println(fmt.Sprintf("Failed to insert data with err=%s", err.Error()))
	}

	ctx := context.TODO()
	var counter int64 = 0
	var retry int = 0
	var results *[][]types.ResultField
done:
	for {
		switch status := iterate(client, ctx, *queryid); status {
		case types.QueryStatusScheduled:
			time.Sleep(500 * time.Millisecond)
			logger.Println("Scheduled")
		case types.QueryStatusRunning:
			var mult int64
			if counter*100 < 2000 {
				mult = counter * 100
				counter += 1
			} else {
				mult = 2000
			}
			t := time.Duration(mult) * time.Millisecond
			if timeParams.max != nil {
				t2 := time.Duration(*timeParams.max) * time.Second
				if t2 > t {
					t = t2
				}
			}
			time.Sleep(t)
			logger.Println("Running")
		case types.QueryStatusComplete:
			logger.Println("Complete")
			output, err := client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{QueryId: queryOutput.QueryId})
			if err != nil {
				logger.Fatalln(fmt.Sprintf("Failed to get results: %s", err.Error()))
			}
			results = &output.Results
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

	if results == nil {
		logger.Fatalln(fmt.Sprintf("unexpectedly no results for queryid=%s", *queryid))
	}

	jsonb, err := json.Marshal(convert(*results))
	if err != nil {
		logger.Fatalln(fmt.Sprintf("Failed to encode result into json due to %s, queryid=%s", err.Error(), *queryid))
	}

	stmt = conn.Prep("insert into results (queryid, result) values ($queryid, $result);")
	stmt.SetText("$queryid", *queryid)
	stmt.SetBytes("$result", jsonb)
	_, err = stmt.Step()
	if err != nil {
		logger.Println(fmt.Sprintf("failed to insert for queryid=%s", *queryid))
		logger.Println(err)
	}
	os.Exit(0)
}
