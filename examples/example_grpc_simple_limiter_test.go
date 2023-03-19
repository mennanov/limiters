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

	"github.com/go-redis/redis"
	"go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mennanov/limiters"
	pb "github.com/mennanov/limiters/examples/helloworld"
)

func Example_simpleGRPCLimiter() {
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

	rate := time.Second * 3
	limiter := limiters.NewTokenBucket(
		2,
		rate,
		limiters.NewLockEtcd(etcdClient, "/ratelimiter_lock/simple/", limiters.NewStdLogger()),
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
