# Distributed rate limiters in Go 
[![Build Status](https://cloud.drone.io/api/badges/mennanov/limiters/status.svg)](https://cloud.drone.io/mennanov/limiters)
[![codecov](https://codecov.io/gh/mennanov/limiters/branch/master/graph/badge.svg)](https://codecov.io/gh/mennanov/limiters)
[![Go Report Card](https://goreportcard.com/badge/github.com/mennanov/limiters)](https://goreportcard.com/report/github.com/mennanov/limiters)
[![GoDoc](https://godoc.org/github.com/mennanov/limiters?status.svg)](https://godoc.org/github.com/mennanov/limiters)

- [`Token bucket`](https://en.wikipedia.org/wiki/Token_bucket)
    - in-memory (local)
    - redis
    - etcd

    Allows requests at a certain input rate with possible bursts configured by the capacity parameter.  
    The output rate equals to the input rate.  
    Precise, but requires a lock (provided).

- [`Leaky bucket`](https://en.wikipedia.org/wiki/Leaky_bucket#As_a_queue)
    - in-memory (local)
    - redis
    - etcd

    Puts requests in a FIFO queue to be processed at a constant rate.  
    There are no restrictions on the input rate except for the capacity of the queue.  
    Requires a lock (provided).

- [`Fixed window counter`](https://konghq.com/blog/how-to-design-a-scalable-rate-limiting-algorithm/)
    - in-memory (local)
    - redis

    Simple and resources efficient algorithm that does not need a lock.  
    Precision may be adjusted by the size of the window.  
    May be lenient when there are many requests around the boundary between 2 adjacent windows.

- [`Sliding window counter`](https://konghq.com/blog/how-to-design-a-scalable-rate-limiting-algorithm/)
    - in-memory (local)
    - redis

    Smoothes out the bursts around the boundary between 2 adjacent windows.  
    Needs as twice more memory as the `Fixed Window` algorithm (2 windows instead of 1 at a time).  
    It will disallow _all_ the requests in case when a client is flooding the service with requests.
    It's the client's responsibility to handle a disallowed request properly: wait before making a new one again.

- `Concurrent buffer`
    - in-memory (local)
    - redis
    
    Allows concurrent requests up to the given capacity.  
    Requires a lock (provided).

## gRPC example

Global token bucket rate limiter for a gRPC service example:
```go
// examples/example_grpc_simple_limiter_test.go
rate := time.Second * 3
limiter := limiters.NewTokenBucket(
    2,
    rate,
    limiters.NewLockerEtcd(etcdClient, "/ratelimiter_lock/simple/", limiters.NewStdLogger()),
    limiters.NewTokenBucketRedis(
        redisClient,
        "ratelimiter/simple",
        rate, false),
    limiters.NewSystemClock(), limiters.NewStdLogger(),
)

// Add a unary interceptor middleware to rate limit all requests.
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

For something close to a real world example see the IP address based gRPC global rate limiter in the 
[examples](examples/example_grpc_ip_limiter_test.go) directory.

## Testing

Run tests locally:
```bash
docker-compose up -d  # start etcd and Redis
ETCD_ENDPOINTS="127.0.0.1:2379" REDIS_ADDR="127.0.0.1:6379" go test
```

Run [Drone](https://drone.io) CI tests locally:
```bash
for p in "go1.13" "go1.12" "go1.11" "lint"; do drone exec --pipeline=${p}; done
```
