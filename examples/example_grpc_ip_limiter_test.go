// Package examples implements a gRPC server for Greeter service using rate limiters.
package examples

import (
	"context"
	"fmt"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/mennanov/limiters"
	pb "github.com/mennanov/limiters/examples/helloworld"
)

func Example_ipGRPCLimiter() {
	// Set up a gRPC server.
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
	// Connect to etcd.
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(os.Getenv("ETCD_ENDPOINTS"), ","),
		DialTimeout: time.Second,
	})
	if err != nil {
		log.Fatalf("could not connect to etcd: %v", err)
	}
	defer etcdClient.Close()
	// Connect to Redis.
	redisClient := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})
	defer redisClient.Close()
	logger := limiters.NewStdLogger()
	// Registry is needed to keep track of previously created limiters. It can remove the expired limiters to free up
	// memory.
	registry := limiters.NewRegistry()
	// The rate is used to define the token bucket refill rate and also the TTL for the limiters (both in Redis and in
	// the registry).
	rate := time.Second * 3
	clock := limiters.NewSystemClock()
	go func() {
		// Garbage collect the old limiters to prevent memory leaks.
		for {
			<-time.After(rate)
			registry.DeleteExpired(clock.Now())
		}
	}()

	// Add a unary interceptor middleware to rate limit requests.
	s := grpc.NewServer(grpc.UnaryInterceptor(
		func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			p, ok := peer.FromContext(ctx)
			var ip string
			if !ok {
				log.Println("no peer info available")
				ip = "unknown"
			} else {
				ip = p.Addr.String()
			}

			// Create an IP address based rate limiter.
			bucket := registry.GetOrCreate(ip, func() interface{} {
				return limiters.NewTokenBucket(
					2,
					rate,
					limiters.NewLockEtcd(etcdClient, fmt.Sprintf("/lock/ip/%s", ip), logger),
					limiters.NewTokenBucketRedis(
						redisClient,
						fmt.Sprintf("/ratelimiter/ip/%s", ip),
						rate, false),
					clock, logger)
			}, rate, clock.Now())
			w, err := bucket.(*limiters.TokenBucket).Limit(ctx)
			if err == limiters.ErrLimitExhausted {
				return nil, status.Errorf(codes.ResourceExhausted, "try again later in %s", w)
			} else if err != nil {
				// The limiter failed. This error should be logged and examined.
				log.Println(err)
				return nil, status.Error(codes.Internal, "internal error")
			}
			return handler(ctx, req)
		}))

	pb.RegisterGreeterServer(s, &server{})
	go func() {
		// Start serving.
		if err = s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	defer s.GracefulStop()

	// Set up a client connection to the server.
	conn, err := grpc.Dial(fmt.Sprintf("localhost%s", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.SayHello(ctx, &pb.HelloRequest{Name: "Alice"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	fmt.Println(r.GetMessage())
	r, err = c.SayHello(ctx, &pb.HelloRequest{Name: "Bob"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	fmt.Println(r.GetMessage())
	r, err = c.SayHello(ctx, &pb.HelloRequest{Name: "Peter"})
	if err == nil {
		log.Fatal("error expected, but got nil")
	}
	fmt.Println(err)
	// Output: Hello Alice
	// Hello Bob
	// rpc error: code = ResourceExhausted desc = try again later in 3s
}
