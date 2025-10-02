package examples

import (
	"context"

	pb "github.com/mennanov/limiters/examples/helloworld"
)

const (
	Port = ":50051"
)

// Server is used to implement helloworld.GreeterServer.
type Server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer.
func (s *Server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

var _ pb.GreeterServer = new(Server)
