package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/kosalaat/file-replicator/pkg/client"
	"github.com/kosalaat/file-replicator/replicator"
)

func TestListen(t *testing.T) {
	server := &ReplicationServer{}
	address := "localhost:50051"
	fileRoot := "./"

	go func() {
		server.StartListening(address, fileRoot)
	}()

	fmt.Println("Creating new client...")
	client, err := client.NewReplicatorClient(address, "./", 10)

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fmt.Println("Ping...")
	pong := client.Ping(context.TODO(), &replicator.PingPong{
		Val: "randomstring",
	})

	if pong.Val != "randomstring" {
		t.Fatalf("Ping failed, expected 'randomstring', got '%s'", pong.Val)
	} else {
		t.Logf("Ping successful, received: %s", pong.Val)
	}

	server.StopListening()

	// Additional checks can be added here to verify server functionality
	t.Logf("Server started successfully on %s with file root %s", address, fileRoot)
}

// func Test
