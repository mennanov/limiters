package limiters

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/pkg/errors"
)

// cosmosTTL converts ttl to the Cosmos DB TTL value: whole seconds rounded up, at least 1 (Cosmos rejects 0 and
// negative values other than -1) and at most math.MaxInt32.
func cosmosTTL(ttl time.Duration) int32 {
	return clampInt32(max(int64(math.Ceil(ttl.Seconds())), 1))
}

type cosmosItem struct {
	Count        int64  `json:"Count"` //nolint:tagliatelle // Persisted field name; renaming would break existing items.
	PartitionKey string `json:"partitionKey"`
	ID           string `json:"id"`
	TTL          int32  `json:"ttl"`
}

// incrementCosmosItemRMW increments the count of a cosmosItem in a read-modify-write loop.
// It uses optimistic concurrency control (ETags) to ensure consistency.
func incrementCosmosItemRMW(ctx context.Context, client *azcosmos.ContainerClient, partitionKey, id string, ttl time.Duration) (int64, error) {
	pk := azcosmos.NewPartitionKey().AppendString(partitionKey)

	for {
		err := ctx.Err()
		if err != nil {
			return 0, err
		}

		resp, err := client.ReadItem(ctx, pk, id, nil)
		if err != nil {
			return 0, errors.Wrap(err, "read of cosmos value failed")
		}

		var existing cosmosItem

		err = json.Unmarshal(resp.Value, &existing)
		if err != nil {
			return 0, errors.Wrap(err, "unmarshal of cosmos value failed")
		}

		existing.Count++
		existing.TTL = cosmosTTL(ttl)

		updated, err := json.Marshal(existing)
		if err != nil {
			return 0, err
		}

		_, err = client.ReplaceItem(ctx, pk, id, updated, &azcosmos.ItemOptions{
			IfMatchEtag: &resp.ETag,
		})
		if err == nil {
			return existing.Count, nil
		}

		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusPreconditionFailed {
			continue
		}

		return 0, errors.Wrap(err, "replace of cosmos value failed")
	}
}
