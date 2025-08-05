package files

import (
	"os"
	"testing"
	// "github.com/kosalaat/file-replicator/pkg/client"
	// "github.com/kosalaat/file-replicator/pkg/server"
)

func Setup(t *testing.T) {
	if err := os.MkdirAll("/tmp/src", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
	if err := os.MkdirAll("/tmp/dest", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}
}

func Teardown(t *testing.T) {
	if err := os.RemoveAll("/tmp/src"); err != nil {
		t.Fatalf("Failed to remove source directory: %v", err)
	}
	if err := os.RemoveAll("/tmp/dest"); err != nil {
		t.Fatalf("Failed to remove destination directory: %v", err)
	}
}
