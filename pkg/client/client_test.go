package client

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/kosalaat/file-replicator/pkg/server"
	"github.com/kosalaat/file-replicator/replicator"
	"github.com/phayes/freeport"
)

func setup(t *testing.T, src string, dest string) {
	if err := os.MkdirAll("/tmp/src", 0755); err != nil {
		t.Fatalf("Failed to create source directory: %v", err)
	}
	if err := os.MkdirAll("/tmp/dest", 0755); err != nil {
		t.Fatalf("Failed to create destination directory: %v", err)
	}

	fsrc, err := os.OpenFile("/tmp/src/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}
	defer fsrc.Close()
	if _, err := fsrc.WriteString(src); err != nil {
		t.Fatalf("Failed to write to source file: %v", err)
	}

	fdest, err := os.OpenFile("/tmp/dest/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}
	defer fdest.Close()

	if _, err := fdest.WriteString(dest); err != nil {
		t.Fatalf("Failed to write to source file: %v", err)
	}
}

func teardown(t *testing.T) {
	if err := os.RemoveAll("/tmp/src"); err != nil {
		t.Fatalf("Failed to remove source directory: %v", err)
	}
	if err := os.RemoveAll("/tmp/dest"); err != nil {
		t.Fatalf("Failed to remove destination directory: %v", err)
	}
}

func TestClient_CheckDuplicates(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqr6tuv7wxy8", "abc1def2ghi3XXX4mno5pqr6YYY7wxy8")
	defer teardown(t)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := server.NewReplicationServer()
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	defer server.StopListening()

	fmt.Println("Creating new client...")
	client, err := NewReplicatorClient(address, "/tmp/src", 10)

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	confirmation, err := client.CheckDuplicates(
		context.TODO(),
		"test.txt",
		4,
	)
	if err != nil {
		t.Fatalf("Failed to check duplicates: %v", err)
	}

	fmt.Printf("Confirmation: %+v\n", confirmation)

	if confirmation.Chunk[0].ChunkID != 3 {
		t.Fatalf("Expected chunk ID 3, got %d", confirmation.Chunk[0].ChunkID)
	}

	if confirmation.Chunk[1].ChunkID != 6 {
		t.Fatalf("Expected chunk ID 3, got %d", confirmation.Chunk[0].ChunkID)
	}

	if len(confirmation.Chunk) != 2 {
		t.Fatalf("Expected 2 chunks, got %d", len(confirmation.Chunk))
	}
}

func TestClient_CheckDuplicatesEmptyfile(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "")
	defer teardown(t)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := server.NewReplicationServer()
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	defer server.StopListening()

	fmt.Println("Creating new client...")
	client, err := NewReplicatorClient(address, "/tmp/src", 10)

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	confirmation, err := client.CheckDuplicates(
		context.TODO(),
		"test.txt",
		4,
	)
	if err != nil {
		t.Fatalf("Failed to check duplicates: %v", err)
	}

	fmt.Printf("Confirmation: %+v, Chunk Length: %d\n", confirmation, len(confirmation.Chunk))

	for _, chunk := range confirmation.Chunk {
		fmt.Printf("Chunk ID: %d, Hash: %v\n", chunk.ChunkID, chunk.Hash)
	}
	if len(confirmation.Chunk) != 9 {
		t.Fatalf("Expected 9 chunks, got %d", len(confirmation.Chunk))
	}
}

func TestClient_ReplicateChunk(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqr6tuv7wxy8", "abc1def2ghi3XXX4mno5pqrs6YYY7wxy8")
	defer teardown(t)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := server.NewReplicationServer()
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	fmt.Println("Creating new client...")
	client, err := NewReplicatorClient(address, "/tmp/src", 10)

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fmt.Println("ReplicateChunk...")
	client.ReplicateChunk(context.TODO(), &replicator.DataPayload{
		DataChunk:        []byte("jkl4"),
		Length:           4,
		ChunkID:          3,
		BlockSize:        4,
		FileMode:         0644,
		RelativeFilePath: "test.txt",
	})

	server.StopListening()

	// Additional checks can be added here to verify server functionality
	t.Logf("Server started successfully on %s with file root %s", address, "dest")
}

func TestClient_ReplicateChunkEmptyFile(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqr6tuv7wxy8", "")
	defer teardown(t)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := server.NewReplicationServer()
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	fmt.Println("Creating new client...")
	client, err := NewReplicatorClient(address, "/tmp/src", 10)

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	data := [][]byte{
		[]byte("abc1"),
		[]byte("def2"),
		[]byte("ghi3"),
		[]byte("jkl4"),
		[]byte("mno5"),
		[]byte("pqr6"),
		[]byte("tuv7"),
		[]byte("wxy8"),
	}

	fmt.Println("ReplicateChunk...")
	for i := 0; i < 8; i++ {
		client.ReplicateChunk(context.TODO(), &replicator.DataPayload{
			DataChunk:        data[i],
			Length:           4,
			ChunkID:          uint64(i),
			BlockSize:        4,
			FileMode:         0644,
			FileSize:         32,
			RelativeFilePath: "test.txt",
		})
	}

	server.StopListening()

	buf, err := os.ReadFile("/tmp/dest/test.txt")

	if err != nil {
		t.Fatalf("Failed to read replicated file: %v", err)
	}
	if string(buf) != "abc1def2ghi3jkl4mno5pqr6tuv7wxy8" {
		t.Fatalf("Expected replicated file content 'abc1def2ghi3jkl4mno5pqr6tuv7wxy8', got '%s'", string(buf))
	} else {
		t.Logf("Replicated file content is as expected: %s", string(buf))
	}
	// Additional checks can be added here to verify server functionality
	t.Logf("Server started successfully on %s with file root %s", address, "dest")
}

func TestOwnershipChange(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8")
	defer teardown(t)

	if err := os.Chmod("/tmp/src/test.txt", 0744); err != nil {
		t.Fatalf("Failed to change file permissions: %v", err)
	}

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := server.NewReplicationServer()
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()
	fmt.Println("Creating new client...")
	client, err := NewReplicatorClient(address, "/tmp/src", 10)

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fmt.Println("ReplicateChunk...")
	client.ReplicateChunk(context.TODO(), &replicator.DataPayload{
		DataChunk:        nil,
		FileMode:         0744,
		RelativeFilePath: "test.txt",
	})
	server.StopListening()

	// Additional checks can be added here to verify server functionality
	t.Logf("Server started successfully on %s with file root %s", address, "dest")

	fileInfo, err := os.Stat("/tmp/dest/test.txt")
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}
	if fileInfo.Mode() != 0744 {
		t.Fatalf("Expected file mode 0744, got %o", fileInfo.Mode())
	}
	t.Logf("File ownership change successful, file mode is now %o", fileInfo.Mode())
}

func TestClient_RenameFile(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8")
	defer teardown(t)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := server.NewReplicationServer()
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	fmt.Println("Creating new client...")
	client, err := NewReplicatorClient(address, "/tmp/src", 10)

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fmt.Println("RenameFile...")
	client.RenameFile(
		context.TODO(),
		"test.txt",
		"renamed_test.txt",
	)

	server.StopListening()

	if _, err := os.Stat("/tmp/dest/renamed_test.txt"); os.IsNotExist(err) {
		t.Fatalf("Renamed file does not exist in destination")
	} else {
		t.Logf("Renamed file exists in destination")
	}
}

func TestClient_DeleteFile(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8")
	defer teardown(t)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := server.NewReplicationServer()
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	fmt.Println("Creating new client...")
	client, err := NewReplicatorClient(address, "/tmp/src", 10)

	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	os.MkdirAll("/tmp/src/first/second", 0755)
	os.MkdirAll("/tmp/dest/first/second", 0755)
	os.Rename("/tmp/src/test.txt", "/tmp/src/first/second/test.txt")
	os.Rename("/tmp/dest/test.txt", "/tmp/dest/first/second/test.txt")

	fmt.Println("DeleteFile...")
	client.DeleteFile(
		context.TODO(),
		"first/second/test.txt",
	)

	server.StopListening()

	if _, err := os.Stat("/tmp/dest/.archive/first/second/test.txt"); err != nil {
		t.Fatalf("Deleted file does not exists in archive destination")
	} else {
		t.Logf("Deleted file does exist in archive destination")
	}
}
