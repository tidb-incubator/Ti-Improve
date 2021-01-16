package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/codepipeline"
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

func HandleRequest(ctx context.Context, event *CodePipelineEvent) (string, error) {
	// Start a new CodePipeline service
	pipeline := codepipeline.New(awsSession)

	params := event.Job.Data.ActionConfiguration.Configuration.UserParameters
	v := &TiDBClusterVar{}
	err := json.Unmarshal([]byte(params), &v)
	if err != nil {
		return "", err
	}
	fmt.Printf("%#v", v)
	input := &codepipeline.PutJobSuccessResultInput{
		JobId: aws.String(event.Job.ID),
	}
	output, err := pipeline.PutJobSuccessResult(input)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello !\n %s\n%s", event.Job.ID, output.String()), nil
}

func main() {

	// Create a new AWS session
	if awsSession == nil {
		awsSession = session.Must(session.NewSession(&aws.Config{
			Region: aws.String("us-east-2"),
		}))
	}
	lambda.Start(HandleRequest)
}
