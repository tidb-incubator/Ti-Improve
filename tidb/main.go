package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/store/mockstore"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	_ "github.com/go-sql-driver/mysql"
	tisession "github.com/pingcap/tidb/session"
)

// Local application variables
var (
	awsSession *session.Session
)

/*
Example of the complete CodePipelineEvent:

{
    "CodePipeline.job": {
        "id": "c0d76431-b0e7-xmpl-97e3-e8ee786eb6f6",
        "accountId": "123456789012",
        "data": {
            "actionConfiguration": {
                "configuration": {
                    "FunctionName": "my-function",
                    "UserParameters": "{\"KEY\": \"VALUE\"}"
                }
            },
            "inputArtifacts": [
                {
                    "name": "my-pipeline-SourceArtifact",
                    "revision": "e0c7xmpl2308ca3071aa7bab414de234ab52eea",
                    "location": {
                        "type": "S3",
                        "s3Location": {
                            "bucketName": "us-west-2-123456789012-my-pipeline",
                            "objectKey": "my-pipeline/test-api-2/TdOSFRV"
                        }
                    }
                }
            ],
            "outputArtifacts": [
                {
                    "name": "invokeOutput",
                    "revision": null,
                    "location": {
                        "type": "S3",
                        "s3Location": {
                            "bucketName": "us-west-2-123456789012-my-pipeline",
                            "objectKey": "my-pipeline/invokeOutp/D0YHsJn"
                        }
                    }
                }
            ],
            "artifactCredentials": {
                "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
                "secretAccessKey": "6CGtmAa3lzWtV7a...",
                "sessionToken": "IQoJb3JpZ2luX2VjEA...",
                "expirationTime": 1575493418000
            }
        }
    }
}
*/

type CodePipelineEvent struct {
	Job struct {
		ID   string `json:"id"`
		Data struct {
			ActionConfiguration struct {
				Configuration struct {
					UserParameters string `json:"UserParameters"`
				} `json:"configuration"`
			} `json:"actionConfiguration"`
		} `json:"data"`
	} `json:"CodePipeline.job"`
}

type TiDBClusterVar struct {
	TiDBPublicIp    string `json:"TiDBPublicIp"`
	TiDBInstanceID  string `json:"TiDBInstanceID"`
	TiKV1InstanceID string `json:"TiKV1InstanceID"`
	TiKV2InstanceID string `json:"TiKV2InstanceID"`
}

type SQLIN struct{
	SQL string `json:"sql"`
}

// {"TiDBPublicIp":"#{DeployVariables.TiDBPublicIp}","TiDBInstanceID":"#{DeployVariables.TiDBInstanceID},"TiKV1InstanceID":"#{DeployVariables.TiKV1InstanceID},"TiKV2InstanceID":"#{DeployVariables.TiKV2InstanceID}"}
func HandleRequest(ctx context.Context, event *SQLIN) (string, error) {
	return exec(ctx,event.SQL)
}

const sqlFileURL = "https://tidb-tests.s3.us-east-2.amazonaws.com/check_new/check.sql.6"
const NUM = 10

func runCheck(ctx context.Context, tidbIP string) error {
	res, err := http.Get(sqlFileURL)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(res.Body)
	defer res.Body.Close()

	db, err := getDB(ctx, tidbIP)
	if err != nil {
		return err
	}
	defer db.Close()

	for i := 0; i < NUM; i++ {
		sql, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			} else {
				return err
			}
		}
		fmt.Println(sql)
		db.Exec(sql)
	}
	return nil
}

const (
	UserName = "root"
	Password = "123456"
	Port     = "4000"
)
var sess tisession.Session

func getDB(ctx context.Context, tidbIP string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", UserName, Password, tidbIP, Port)
	return sql.Open("mysql", dsn)
}

func getSession(ctx context.Context) (tisession.Session,error){
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil,err
	}
	//defer store.Close() //nolint:errcheck
	tisession.SetSchemaLease(0)
	tisession.DisableStats4Test()
	domain, err := tisession.BootstrapSession(store)
	if err != nil {
		return nil,err
	}
	//defer domain.Close()
	domain.SetStatsUpdating(true)
	return tisession.CreateSession4Test(store)
}

func exec(ctx context.Context,sql string) (res string,err error){
	rss,err:=sess.Execute(ctx,sql)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	for i,rs:=range rss{
		sb.WriteString(fmt.Sprintf("------RecordSet(%d)------\n",i))
		sRows, err := tisession.ResultSetToStringSlice(ctx, sess, rs)
		if err != nil {
			return "", err
		}
		for _,row:=range sRows{
			for _,c:=range row{
				sb.WriteString(c)
				sb.WriteString("    ")
			}
			sb.WriteString("\n")
		}
	}
	return sb.String(), nil
}

func main() {
	mysql.TiDBReleaseVersion="v4.0.9-aws-lambda"
	if sess==nil{
		session,err:=getSession(context.Background())
		if err !=nil{
			panic(err)
		}
		sess=session
	}
	//print(exec(context.Background(),"select tidb_version();"))
	lambda.Start(HandleRequest)
}
