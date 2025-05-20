package limiters

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pkg/errors"
)

// DynamoDBTableProperties are supplied to DynamoDB limiter backends.
// This struct informs the backend what the name of the table is and what the names of the key fields are.
type DynamoDBTableProperties struct {
	// TableName is the name of the table.
	TableName string
	// PartitionKeyName is the name of the PartitionKey attribute.
	PartitionKeyName string
	// SortKeyName is the name of the SortKey attribute.
	SortKeyName string
	// SortKeyUsed indicates if a SortKey is present on the table.
	SortKeyUsed bool
	// TTLFieldName is the name of the attribute configured for TTL.
	TTLFieldName string
}

// LoadDynamoDBTableProperties fetches a table description with the supplied client and returns a DynamoDBTableProperties struct.
func LoadDynamoDBTableProperties(ctx context.Context, client *dynamodb.Client, tableName string) (DynamoDBTableProperties, error) {
	resp, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		return DynamoDBTableProperties{}, errors.Wrap(err, "describe dynamodb table failed")
	}

	ttlResp, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: &tableName,
	})
	if err != nil {
		return DynamoDBTableProperties{}, errors.Wrap(err, "describe dynamobd table ttl failed")
	}

	data, err := loadTableData(resp.Table, ttlResp.TimeToLiveDescription)
	if err != nil {
		return data, err
	}

	return data, nil
}

func loadTableData(table *types.TableDescription, ttl *types.TimeToLiveDescription) (DynamoDBTableProperties, error) {
	data := DynamoDBTableProperties{
		TableName: *table.TableName,
	}

	data, err := loadTableKeys(data, table)
	if err != nil {
		return data, errors.Wrap(err, "invalid dynamodb table")
	}

	return populateTableTTL(data, ttl), nil
}

func loadTableKeys(data DynamoDBTableProperties, table *types.TableDescription) (DynamoDBTableProperties, error) {
	for _, key := range table.KeySchema {
		if key.KeyType == types.KeyTypeHash {
			data.PartitionKeyName = *key.AttributeName

			continue
		}

		data.SortKeyName = *key.AttributeName
		data.SortKeyUsed = true
	}

	for _, attribute := range table.AttributeDefinitions {
		name := *attribute.AttributeName
		if name != data.PartitionKeyName && name != data.SortKeyName {
			continue
		}

		if name == data.PartitionKeyName && attribute.AttributeType != types.ScalarAttributeTypeS {
			return data, errors.New("dynamodb partition key must be of type S")
		} else if data.SortKeyUsed && name == data.SortKeyName && attribute.AttributeType != types.ScalarAttributeTypeS {
			return data, errors.New("dynamodb sort key must be of type S")
		}
	}

	return data, nil
}

func populateTableTTL(data DynamoDBTableProperties, ttl *types.TimeToLiveDescription) DynamoDBTableProperties {
	if ttl.TimeToLiveStatus != types.TimeToLiveStatusEnabled {
		return data
	}

	data.TTLFieldName = *ttl.AttributeName

	return data
}

func dynamoDBputItem(ctx context.Context, client *dynamodb.Client, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	resp, err := client.PutItem(ctx, input)
	if err != nil {
		var cErr *types.ConditionalCheckFailedException
		if errors.As(err, &cErr) {
			return nil, ErrRaceCondition
		}

		return nil, errors.Wrap(err, "unable to set dynamodb item")
	}

	return resp, nil
}

func dynamoDBGetItem(ctx context.Context, client *dynamodb.Client, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	input.ConsistentRead = aws.Bool(true)

	var resp *dynamodb.GetItemOutput
	var err error

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err = client.GetItem(ctx, input)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to retrieve dynamodb item")
	}

	return resp, nil
}
