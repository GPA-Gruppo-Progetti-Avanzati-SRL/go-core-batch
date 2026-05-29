package grpc

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc/proto"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	client proto.DistributionChannelClient
}

func NewClient(config *ClientConfig) *Client {
	conn, err := gogrpc.NewClient(config.Url,
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		gogrpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Impossibile creare gRPC client")
	}
	return &Client{client: proto.NewDistributionChannelClient(conn)}
}

func (g *Client) DistribuiteSimpleTask(jobId string) (string, error) {
	tuuid, _ := uuid.NewRandom()
	taskId := tuuid.String()
	t, err := g.client.DistribuiteSimpleTask(context.Background(), &proto.SampleTaskMessage{
		TaskId: taskId,
		JobId:  jobId,
	})
	if err != nil {
		log.Error().Err(err).Msgf("Errore chiamata gRPC DistribuiteSimpleTask: %s", err.Error())
		return taskId, err
	}
	log.Info().Msgf("S - %s - %s - Task distribuito su %s", jobId, taskId, t.Hostname)
	return taskId, nil
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
