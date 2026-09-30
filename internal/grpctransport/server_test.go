package grpctransport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	batchgrpc "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/grpc"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
)

type nopShutdowner struct{}

func (nopShutdowner) Shutdown(...fx.ShutdownOption) error { return nil }

// TestServer_StopRispettaLaDeadline: un RPC che non termina non deve tenere in piedi lo stop oltre
// il context dell'hook. Prima OnStop chiamava GracefulStop, che aspetta ogni RPC in volo e non
// conosce il context.
func TestServer_StopRispettaLaDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	lc := fxtest.NewLifecycle(t)
	NewServer(lc, nopShutdowner{}, &batchgrpc.ServerConfig{Hostname: "127.0.0.1", Port: port})
	lc.RequireStart()

	conn, err := gogrpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), gogrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Lo stream di reflection resta aperto finché il client non lo chiude: è l'RPC appeso.
	stream, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reflectionpb.ServerReflectionRequest{MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := lc.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Stop durato %v: la deadline di 300ms non è stata rispettata", d)
	}
}
