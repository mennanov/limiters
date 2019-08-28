# Distributed rate limiters in Go 
[![Build Status](https://cloud.drone.io/api/badges/mennanov/limiters/status.svg)](https://cloud.drone.io/mennanov/limiters)
[![codecov](https://codecov.io/gh/mennanov/limiters/branch/master/graph/badge.svg)](https://codecov.io/gh/mennanov/limiters)
[![Go Report Card](https://goreportcard.com/badge/github.com/mennanov/limiters)](https://goreportcard.com/report/github.com/mennanov/limiters)
[![GoDoc](https://godoc.org/github.com/mennanov/limiters?status.svg)](https://godoc.org/github.com/mennanov/limiters)

- [Token bucket](#Token bucket)
    - in-memory
    - redis
    - etcd
- [Leaky bucket](#Leaky bucket)
    - in-memory
    - redis
    - etcd
- Sliding log (work in progress)
- Sliding window (work in progress)

See the comments in the source code for details on each algorithm's backend implementation for possible caveats and 
use cases.

## Distributed locks
- in-memory
- etcd
- Consul (work in progress)
- Zookeeper (work in progress)

## gRPC example

Global rate limiter for a gRPC service example:
```go
limiter := limiters.NewTokenBucket(
    limiters.NewLockerEtcd(etcdClient, "/ratelimiter_lock/", logger),
    limiters.NewTokenBucketRedis(
        redisClient,
        "ratelimiter",
        // Allowance: 1 request per 3 seconds with the burst of size 2.
        limiters.TokenBucketState{
            RefillRate: rate,
            Capacity:   2,
            Available:  2,
        },
        time.Second * 3),
    limiters.NewSystemClock(), limiters.NewStdLogger(),
)
s := grpc.NewServer(grpc.UnaryInterceptor(
    func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
        w, err := limiter.Limit(ctx)
        if err == limiters.ErrLimitExhausted {
            return nil, status.Errorf(codes.ResourceExhausted, "try again later in %s", w)
        } else if err != nil {
            // The limiter failed. This error should be logged and examined.
            log.Println(err)
            return nil, status.Error(codes.Internal, "internal error")
        }
        return handler(ctx, req)
    }))
```

Also see the IP address based gRPC global rate limiter in the [examples](examples/example_grpc_test.go) directory.

## Testing

Run tests locally:
```bash
docker-compose up -d  # start etcd and Redis
ETCD_ENDPOINTS="127.0.0.1:2379" REDIS_ADDR="127.0.0.1:6379" go test
```

Run [Drone](https://drone.io) CI tests locally:
```bash
for p in "go1.12" "go1.11" "go1.10" "go1.9" "lint"; do drone exec --pipeline=${p}; done
```

---
### Token bucket
[Token bucket](https://en.wikipedia.org/wiki/Token_bucket) allows requests at a certain input rate with possible bursts
configured by the capacity parameter. The output rate equals to the input rate.

Use case: restrict an access to a service up to a certain rate (e.g. 10 requests per second) while allowing the requests
to arrive in batches (bursts).

### Leaky bucket
[Leaky bucket](https://en.wikipedia.org/wiki/Leaky_bucket#As_a_queue) puts requests in a FIFO queue to be processed
at a certain constant rate. There are no restrictions on the input rate except for the capacity of the queue.

Use case: process requests at a constant predictable rate.
