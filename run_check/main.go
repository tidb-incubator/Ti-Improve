package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/codepipeline"
	_ "github.com/go-sql-driver/mysql"
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

// {"TiDBPublicIp":"#{DeployVariables.TiDBPublicIp}","TiDBInstanceID":"#{DeployVariables.TiDBInstanceID},"TiKV1InstanceID":"#{DeployVariables.TiKV1InstanceID},"TiKV2InstanceID":"#{DeployVariables.TiKV2InstanceID}"}
func HandleRequest(ctx context.Context, event *CodePipelineEvent) (string, error) {
	// Start a new CodePipeline service
	pipeline := codepipeline.New(awsSession)
	println(1)
	params := event.Job.Data.ActionConfiguration.Configuration.UserParameters
	println(params)
	v := &TiDBClusterVar{}
	err := json.Unmarshal([]byte(params), &v)
	if err != nil {
		return "", err
	}
	fmt.Sprintf("%#v",v)
	err=runCheck(ctx, v.TiDBPublicIp)
	if err != nil {
		return "", err
	}
	println(2)
	input := &codepipeline.PutJobSuccessResultInput{
		JobId: aws.String(event.Job.ID),
	}
	output, err := pipeline.PutJobSuccessResult(input)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello !\n %s\n%s", event.Job.ID, output.String()), nil
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

func getDB(ctx context.Context, tidbIP string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", UserName, Password, tidbIP, Port)
	return sql.Open("mysql", dsn)
}

func main() {
	// runCheck(context.Background(), "3.17.206.94")
	// Create a new AWS session
	if awsSession == nil {
		awsSession = session.Must(session.NewSession(&aws.Config{
			Region: aws.String("us-east-2"),
		}))
	}
	lambda.Start(HandleRequest)
}
