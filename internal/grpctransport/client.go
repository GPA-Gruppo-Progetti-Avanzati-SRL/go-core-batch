// Package grpctransport contiene client e server gRPC del framework batch.
// È un package INTERNAL: importabile solo da codice dentro go-core-batch
// (grpcdispatcher lato client, grpchandler lato server), NON dalle applicazioni.
// I tipi di config pubblici (ClientConfig/ServerConfig/Config) restano nel package grpc.
package grpctransport

import (
	"context"
	"fmt"

	batchgrpc "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc/proto"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	client proto.DistributionChannelClient
}

// NewClient è un costruttore fx: ritorna errore invece di log.Fatal, così un URL/config non
// valido fa fallire lo startup in modo pulito invece di terminare il processo dalla libreria.
func NewClient(config *batchgrpc.ClientConfig) (*Client, error) {
	conn, err := gogrpc.NewClient(config.Url,
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		gogrpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("creazione gRPC client (url=%q): %w", config.Url, err)
	}
	return &Client{client: proto.NewDistributionChannelClient(conn)}, nil
}

func (g *Client) DistribuiteTask(ctx context.Context, jobId, taskId, objectId, taskType string) (string, error) {
	t, err := g.client.DistribuiteTask(ctx, &proto.TaskMessage{
		TaskId:   taskId,
		JobId:    jobId,
		ObjectId: objectId,
		TaskType: taskType,
	})
	if err != nil {
		log.Error().Err(err).Msgf("Errore chiamata gRPC DistribuiteTask: %s", err.Error())
		return taskId, err
	}
	log.Info().Msgf("S - %s - %s - Task distribuito su %s", jobId, taskId, t.Hostname)
	return taskId, nil
}
