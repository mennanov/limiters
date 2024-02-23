# Distributed rate limiters for Golang 
[![Build Status](https://github.com/mennanov/limiters/actions/workflows/tests.yml/badge.svg)](https://github.com/mennanov/limiters/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/mennanov/limiters/branch/master/graph/badge.svg?token=LZULu4i7B6)](https://codecov.io/gh/mennanov/limiters)
[![Go Report Card](https://goreportcard.com/badge/github.com/mennanov/limiters)](https://goreportcard.com/report/github.com/mennanov/limiters)
[![GoDoc](https://godoc.org/github.com/mennanov/limiters?status.svg)](https://godoc.org/github.com/mennanov/limiters)

Rate limiters for distributed applications in Golang with configurable back-ends and distributed locks.  
Any types of back-ends and locks can be used that implement certain minimalistic interfaces. 
Most common implementations are already provided.  

- [`Token bucket`](https://en.wikipedia.org/wiki/Token_bucket)
    - in-memory (local)
    - redis
    - etcd
    - dynamodb
    - memcached

    Allows requests at a certain input rate with possible bursts configured by the capacity parameter.  
    The output rate equals to the input rate.  
    Precise (no over or under-limiting), but requires a lock (provided).

- [`Leaky bucket`](https://en.wikipedia.org/wiki/Leaky_bucket#As_a_queue)
    - in-memory (local)
    - redis
    - etcd
    - dynamodb
    - memcached

    Puts requests in a FIFO queue to be processed at a constant rate.  
    There are no restrictions on the input rate except for the capacity of the queue.  
    Requires a lock (provided).

- [`Fixed window counter`](https://konghq.com/blog/how-to-design-a-scalable-rate-limiting-algorithm/)
    - in-memory (local)
    - redis
    - dynamodb
    - memcached

    Simple and resources efficient algorithm that does not need a lock.  
    Precision may be adjusted by the size of the window.  
    May be lenient when there are many requests around the boundary between 2 adjacent windows.

- [`Sliding window counter`](https://konghq.com/blog/how-to-design-a-scalable-rate-limiting-algorithm/)
    - in-memory (local)
    - redis
    - dynamodb
    - memcached

    Smoothes out the bursts around the boundary between 2 adjacent windows.  
    Needs as twice more memory as the `Fixed Window` algorithm (2 windows instead of 1 at a time).  
    It will disallow _all_ the requests in case when a client is flooding the service with requests.
    It's the client's responsibility to handle a disallowed request properly: wait before making a new one again.

- `Concurrent buffer`
    - in-memory (local)
    - redis
    - memcached
    
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

## DynamoDB

The use of DynamoDB requires the creation of a DynamoDB Table prior to use. An existing table can be used or a new one can be created. Depending on the limiter backend:

* Partion Key
  - String
  - Required for all Backends
* Sort Key
  - String
  - Backends:
    - FixedWindow
    - SlidingWindow
* TTL
  - Number
  - Backends:
    - FixedWindow
    - SlidingWindow
    - LeakyBucket
    - TokenBucket

All DynamoDB backends accept a `DynamoDBTableProperties` struct as a paramater. This can be manually created or use the `LoadDynamoDBTableProperties` with the table name. When using `LoadDynamoDBTableProperties`, the table description is fetched from AWS and verified that the table can be used for Limiter backends. Results of `LoadDynamoDBTableProperties` are cached.

## Distributed locks

Some algorithms require a distributed lock to guarantee consistency during concurrent requests.  
In case there is only 1 running application instance then no distributed lock is needed 
as all the algorithms are thread-safe (use `LockNoop`).

Supported backends:
- [etcd](https://etcd.io/)
- [Consul](https://www.consul.io/)
- [Zookeeper](https://zookeeper.apache.org/)
- [Redis](https://redis.io/)
- [Memcached](https://memcached.org/)

## Testing

Run tests locally:
```bash
docker-compose up -d  # start etcd, Redis, zookeeper, consul, memcached, and localstack
ETCD_ENDPOINTS="127.0.0.1:2379" REDIS_ADDR="127.0.0.1:6379" ZOOKEEPER_ENDPOINTS="127.0.0.1" CONSUL_ADDR="127.0.0.1:8500" AWS_ADDR="127.0.0.1:8000" MEMCACHED_ADDR="127.0.0.1:11211" go test -race -v
```

Run [Drone](https://drone.io) CI tests locally:
```bash
for p in "go1.13" "go1.12" "go1.11" "lint"; do drone exec --pipeline=${p}; done
```
