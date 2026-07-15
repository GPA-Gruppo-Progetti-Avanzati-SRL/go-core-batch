package grpc

import (
	"context"
	"errors"
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

func NewServer(lc fx.Lifecycle, sh fx.Shutdowner, config *ServerConfig) *Server {
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
			go func() {
				// Serve blocca fino allo stop. Su GracefulStop ritorna nil o
				// ErrServerStopped: in quei casi è uno shutdown normale. Un altro
				// errore significa server morto → logga ed escala lo shutdown
				// dell'app (non lasciare un processo vivo senza worker gRPC).
				if serveErr := grpcServer.Serve(lis); serveErr != nil && !errors.Is(serveErr, gogrpc.ErrServerStopped) {
					log.Error().Err(serveErr).Msg("grpc Server terminato con errore, shutdown dell'app")
					if shErr := sh.Shutdown(); shErr != nil {
						log.Error().Err(shErr).Msg("Shutdown dell'app fallito")
					}
				}
			}()
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
