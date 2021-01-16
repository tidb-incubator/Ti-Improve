package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/codepipeline"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

type TiDBInstanceVar struct {
	TiDBPublicIp      string `json:"TiDBPublicIp"`
	TiDBInstanceID     string `json:"TiDBInstanceID"`
	TiKV1InstanceID     string `json:"TiKV1InstanceID"`
	TiKV2InstanceID     string `json:"TiKV2InstanceID"`
}

func HandleRequest(ctx context.Context, event *CodePipelineEvent) (string, error) {
	// receive parameters from pipeline
	pipeline := codepipeline.New(awsSession)
	input := &codepipeline.PutJobSuccessResultInput{
		JobId: aws.String(event.Job.ID),
	}
	params := event.Job.Data.ActionConfiguration.Configuration.UserParameters
	v := &TiDBInstanceVar{}
	err := json.Unmarshal([]byte(params), &v)
	if err != nil {
		return "", err
	}
	fmt.Printf("%#v", v)

	// Start a new CodePipeline service
	// download go-tpc
	// The session the S3 Downloader will use
	sess := session.Must(session.NewSession())

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)

	// Create a file to write the S3 Object contents to.
	filename := "/tmp/go-tpc"
	f, err := os.Create(filename)
	if err != nil {
		return "", fmt.Errorf("failed to create file %q, %v", filename, err)
	}

	// Write the contents of S3 Object to the file
	n, err := downloader.Download(f, &s3.GetObjectInput{
		Bucket: aws.String("tidb-tests"),
		Key:    aws.String("go-tpc"),
	})
	if err != nil {
		return "", fmt.Errorf("failed to download file, %v", err)
	}
	fmt.Printf("file downloaded, %d bytes\n", n)
	err = f.Chmod(0770)
	if err != nil {
		log.Fatal(err)
	}
	err = f.Close()
	if err != nil {
		log.Fatal(err)
	}

	cmd := exec.Command("ls", "-la", "/tmp")
	var lsout bytes.Buffer
	cmd.Stdout = &lsout
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("result: %q\n", lsout.String())

	// load tpcc data
	//runCmd := exec.Command("/tmp/go-tpc", "tpcc", "--warehouses", "1", "-H", v.TiDBPublicIp, "-P", "4000", "-D", "tpcc", "prepare")
	//var out1 bytes.Buffer
	//runCmd.Stdout = &out1
	//err = runCmd.Run()
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Printf("run sql: %q\n", out1.String())

	// run go-tpc
	runCmd := exec.Command("/tmp/go-tpc", "tpcc", "--warehouses", "1", "-H", v.TiDBPublicIp, "-P", "4000", "-D", "tpcc", "--time", "60s", "run")
	var out2 bytes.Buffer
	runCmd.Stdout = &out2
	err = runCmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("run sql: %q\n", out2.String())

	// process output
	result := out2.String()
	var tpm float64
	for _, line := range strings.Split(result, "\n") {
		// find the summary line
		if len(line) > len("tpmC") {
			if line[0:len("tpmC")] == "tpmC" {
				fmt.Println(line)
				fields := strings.Split(line, ",")
				tpmString := strings.Split(fields[0], ":")[1]
				tpmString = strings.Replace(tpmString, " ", "", -1)
				fmt.Println(tpmString)
				tpm, _ = strconv.ParseFloat(tpmString, 64)
			}
		}
	}
	fmt.Printf("transaction per minutes %v\n", tpm)

	// send metric to cloudWatch
	watchSess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create new cloudwatch client.
	watchSvc := cloudwatch.New(watchSess)
	_, err = watchSvc.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Benchmark"),
		MetricData: []*cloudwatch.MetricDatum{
			&cloudwatch.MetricDatum{
				MetricName: aws.String("TransactionPerMinutes"),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(tpm),
				Dimensions: []*cloudwatch.Dimension{
					&cloudwatch.Dimension{
						Name:  aws.String("tpcc"),
						Value: aws.String("1"),
					},
				},
			},
		},
	})
	if err != nil {
		fmt.Println("Error adding metrics:", err.Error())
		return "", err
	}

	output, err := pipeline.PutJobSuccessResult(input)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello !\n %s\n%s", event.Job.ID,output.String()), nil
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
