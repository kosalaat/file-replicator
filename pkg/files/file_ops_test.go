package files

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/kosalaat/file-replicator/pkg/client"
	"github.com/kosalaat/file-replicator/pkg/server"
	"github.com/phayes/freeport"
)

func setup(t *testing.T, src string, dest string) {
	if err := os.MkdirAll("/tmp/src", 0755); err != nil {
		t.Fatalf("Failed to create source directory: %v", err)
	}
	if err := os.MkdirAll("/tmp/dest", 0755); err != nil {
		t.Fatalf("Failed to create destination directory: %v", err)
	}

	fsrc, err := os.OpenFile("/tmp/src/test.txt", os.O_CREATE|os.O_WRONLY, 0774)
	if err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}
	defer fsrc.Close()
	if _, err := fsrc.WriteString(src); err != nil {
		t.Fatalf("Failed to write to source file: %v", err)
	}

	fdest, err := os.OpenFile("/tmp/dest/test.txt", os.O_CREATE|os.O_WRONLY, 0774)
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

func TestFileReplicator_ProcessFile(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "abc1def2ghi3XXX4mno5pqrs6YYY7wxy8")
	defer teardown(t)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := &server.ReplicationServer{}
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	replicatorClient, err := client.NewReplicatorClient(address, "/tmp/src", 10)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fileReplicator := &FileReplicator{
		ReplicatorClient: *replicatorClient,
	}

	err = fileReplicator.ProcessFile("test.txt", 10)
	if err != nil {
		t.Fatalf("ProcessFile failed: %v", err)
	}

	server.StopListening()

	fSRC, err := os.Open("/tmp/src/test.txt")
	if err != nil {
		t.Fatalf("Source file does not exist: %v", err)
	}
	fDEST, err := os.Open("/tmp/dest/test.txt")
	if err != nil {
		t.Fatalf("Destination file does not exist: %v", err)
	}
	defer fSRC.Close()
	defer fDEST.Close()

	bufSRC := make([]byte, 1024)
	bufDEST := make([]byte, 1024)
	var nSRC, nDEST int
	if nSRC, err = fSRC.Read(bufSRC); err != nil {
		t.Fatalf("Failed to read source file: %v", err)
	}
	if nDEST, err = fDEST.Read(bufDEST); err != nil {
		t.Fatalf("Failed to read destination file: %v", err)
	}
	if string(bufSRC[:nSRC]) != string(bufDEST[:nDEST]) {
		t.Fatalf("Source str: %s, destination str: %s, not matching", string(bufSRC[:nSRC]), string(bufDEST[:nDEST]))
	} else {
		t.Logf("Source str: %s, destination str: %s, matching", string(bufSRC[:nSRC]), string(bufDEST[:nDEST]))
	}
}

func TestFileReplicator_ProcessFileEmptyFile(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "")
	defer teardown(t)

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := &server.ReplicationServer{}
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	replicatorClient, err := client.NewReplicatorClient(address, "/tmp/src", 10)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fileReplicator := &FileReplicator{
		ReplicatorClient: *replicatorClient,
	}

	err = fileReplicator.ProcessFile("test.txt", 10)
	if err != nil {
		t.Fatalf("ProcessFile failed: %v", err)
	}

	server.StopListening()

	fSRC, err := os.Open("/tmp/src/test.txt")
	if err != nil {
		t.Fatalf("Source file does not exist: %v", err)
	}
	fDEST, err := os.Open("/tmp/dest/test.txt")
	if err != nil {
		t.Fatalf("Destination file does not exist: %v", err)
	}
	defer fSRC.Close()
	defer fDEST.Close()

	bufSRC := make([]byte, 1024)
	bufDEST := make([]byte, 1024)
	var nSRC, nDEST int
	if nSRC, err = fSRC.Read(bufSRC); err != nil {
		t.Fatalf("Failed to read source file: %v", err)
	}
	if nDEST, err = fDEST.Read(bufDEST); err != nil {
		t.Fatalf("Failed to read destination file: %v", err)
	}
	if string(bufSRC[:nSRC]) != string(bufDEST[:nDEST]) {
		t.Fatalf("Source str: %s, destination str: %s, not matching", string(bufSRC[:nSRC]), string(bufDEST[:nDEST]))
	} else {
		t.Logf("Source str: %s, destination str: %s, matching", string(bufSRC[:nSRC]), string(bufDEST[:nDEST]))
	}
}

func TestOwnerShipChange(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "abc1def2ghi3XXX4mno5pqrs6YYY7wxy8")
	defer teardown(t)

	if err := os.Chmod("/tmp/src/test.txt", 0775); err != nil {
		t.Fatalf("Failed to change permissions of source file: %v", err)
	}

	if err := os.Chown("/tmp/src/test.txt", -1, 12); err != nil {
		t.Logf("Failed to change ownership of source file: %v", err)
	} else {

		port, err := freeport.GetFreePort()
		if err != nil {
			t.Fatalf("Failed to get free port: %v", err)
		}
		server := &server.ReplicationServer{}
		address := fmt.Sprintf("127.0.0.1:%d", port)

		go func() {
			server.StartListening(address, "/tmp/dest")
		}()

		replicatorClient, err := client.NewReplicatorClient(address, "/tmp/src", 10)
		if err != nil {
			t.Fatalf("Failed to create client: %v", err)
		}

		fileReplicator := &FileReplicator{
			ReplicatorClient: *replicatorClient,
		}

		err = fileReplicator.UpdateOwnership("test.txt")
		if err != nil {
			t.Fatalf("ProcessFile failed: %v", err)
		}

		server.StopListening()

		stat, _ := os.Stat("/tmp/dest/test.txt")
		if stat.Mode() != 0775 {
			t.Fatalf("Ownership change failed, expected 0775, got %o", stat.Mode())
		} else {
			t.Logf("Ownership change successful, permissions: %o", stat.Mode())
		}
		if stat_t := stat.Sys().(*syscall.Stat_t); stat_t.Gid != 12 {
			t.Fatalf("Ownership change failed, expected UID: 99999, GID: 99999, got UID: %d, GID: %d", stat_t.Uid, stat_t.Gid)
		} else {
			t.Logf("Ownership change successful, UID: %d, GID: %d", stat_t.Uid, stat_t.Gid)
		}
	}
}

func TestRenameFile(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8")
	defer teardown(t)

	os.MkdirAll("/tmp/src/first/second", 0755)
	os.MkdirAll("/tmp/dest/first/second", 0755)
	os.Rename("/tmp/src/test.txt", "/tmp/src/first/second/test.txt")
	os.Rename("/tmp/dest/test.txt", "/tmp/dest/first/second/test.txt")

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := &server.ReplicationServer{}
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	replicatorClient, err := client.NewReplicatorClient(address, "/tmp/src", 10)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fileReplicator := &FileReplicator{
		ReplicatorClient: *replicatorClient,
	}

	fileReplicator.RenameFile("first/second/test.txt", "first/second/renamed_test.txt")
	if _, err := os.Stat("/tmp/dest/first/second/renamed_test.txt"); os.IsNotExist(err) {
		t.Fatalf("Renamed file does not exist in destination")
	} else {
		t.Logf("Renamed file exists in destination")
	}
	server.StopListening()
}

func TestDeleteFile(t *testing.T) {
	setup(t, "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8", "abc1def2ghi3jkl4mno5pqrs6tuv7wxy8")
	defer teardown(t)

	os.MkdirAll("/tmp/src/first/second", 0755)
	os.MkdirAll("/tmp/dest/first/second", 0755)
	os.Rename("/tmp/src/test.txt", "/tmp/src/first/second/test.txt")
	os.Rename("/tmp/dest/test.txt", "/tmp/dest/first/second/test.txt")

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("Failed to get free port: %v", err)
	}
	server := &server.ReplicationServer{}
	address := fmt.Sprintf("127.0.0.1:%d", port)

	go func() {
		server.StartListening(address, "/tmp/dest")
	}()

	replicatorClient, err := client.NewReplicatorClient(address, "/tmp/src", 10)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fileReplicator := &FileReplicator{
		ReplicatorClient: *replicatorClient,
	}

	fileReplicator.DeleteFile("first/second/test.txt")
	if _, err := os.Stat("/tmp/dest/.archive/first/second/test.txt"); err != nil {
		t.Fatalf("Failed to find archived file in destination: %v", err)
	} else {
		t.Logf("Deleted file exist in archive destination")
	}
	server.StopListening()
}
