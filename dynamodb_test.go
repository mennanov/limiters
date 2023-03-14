package limiters_test

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

const testDynamoDBTableName = "limiters-test"

func CreateTestDynamoDBTable(ctx context.Context, client *dynamodb.Client) error {
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(testDynamoDBTableName),
		BillingMode: types.BillingModePayPerRequest,
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("PK"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("SK"),
				KeyType:       types.KeyTypeRange,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("PK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("SK"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "create test dynamodb table failed")
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(testDynamoDBTableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("TTL"),
			Enabled:       aws.Bool(true),
		},
	})
	if err != nil {
		return errors.Wrap(err, "set dynamodb ttl failed")
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	for {
		resp, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(testDynamoDBTableName),
		})

		if err == nil {
			return errors.Wrap(err, "failed to describe test table")
		}

		if resp.Table.TableStatus == types.TableStatusActive {
			return nil
		}

		select {
		case <-ctx.Done():
			return errors.New("failed to verify dynamodb test table is created")
		default:
		}
	}
}

func DeleteTestDynamoDBTable(ctx context.Context, client *dynamodb.Client) error {
	_, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(testDynamoDBTableName),
	})

	if err != nil {
		return errors.Wrap(err, "delete test dynamodb table failed")
	}

	return nil
}
