package limiters_test

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

const (
	testCosmosDBName        = "limiters-db-test"
	testCosmosContainerName = "limiters-container-test"
)

var defaultTTL int32 = 86400

func CreateCosmosDBContainer(ctx context.Context, client *azcosmos.Client) error {
	resp, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{
		ID: testCosmosDBName,
	}, &azcosmos.CreateDatabaseOptions{})
	if err != nil {
		return err
	}

	dbClient, err := client.NewDatabase(resp.DatabaseProperties.ID)
	if err != nil {
		return err
	}

	_, err = dbClient.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID:                testCosmosContainerName,
		DefaultTimeToLive: &defaultTTL,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{`/partitionKey`},
		},
	}, &azcosmos.CreateContainerOptions{})
	return err
}

func DeleteCosmosDBContainer(ctx context.Context, client *azcosmos.Client) error {
	dbClient, err := client.NewDatabase(testCosmosDBName)
	if err != nil {
		return err
	}

	_, err = dbClient.Delete(ctx, &azcosmos.DeleteDatabaseOptions{})
	return err
}
