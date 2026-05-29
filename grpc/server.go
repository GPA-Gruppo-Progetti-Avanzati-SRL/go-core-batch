package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.uber.org/fx"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var tracer = otel.Tracer("grpc-server")

type Server struct {
	*gogrpc.Server
}

func NewServer(lc fx.Lifecycle, config *ServerConfig) *Server {
	opts := []gogrpc.ServerOption{
		gogrpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: time.Minute * 5,
		}),
		gogrpc.StatsHandler(otelgrpc.NewServerHandler()),
	}
	grpcServer := gogrpc.NewServer(opts...)
	reflection.Register(grpcServer)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info().Msg("Avvio grpc Server")
			lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.Hostname, config.Port))
			if err != nil {
				return err
			}
			go grpcServer.Serve(lis)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info().Msg("Stopping grpc Server")
			grpcServer.GracefulStop()
			return nil
		},
	})
	return &Server{grpcServer}
}
