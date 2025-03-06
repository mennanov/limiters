package limiters

type cosmosItem struct {
	Count        int64  `json:"Count"`
	PartitionKey string `json:"partitionKey"`
	ID           string `json:"id"`
	TTL          int32  `json:"ttl"`
}
