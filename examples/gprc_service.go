package examples

import (
	"context"

	pb "github.com/mennanov/limiters/examples/helloworld"
)

const (
	port = ":50051"
)

// server is used to implement helloworld.GreeterServer.
type server struct{}

// SayHello implements helloworld.GreeterServer.
func (s *server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

// SayGoodbye implements helloworld.GreeterServer.
func (s *server) SayGoodbye(_ context.Context, in *pb.GoodbyeRequest) (*pb.GoodbyeReply, error) {
	return &pb.GoodbyeReply{Message: "Hello " + in.GetName()}, nil
}

var _ pb.GreeterServer = new(server)
