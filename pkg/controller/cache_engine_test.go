package controller

import (
	"os"
	"testing"
)

func setup(t *testing.T, src string) {
	if err := os.MkdirAll("/tmp/src", 0755); err != nil {
		t.Fatalf("Failed to create source directory: %v", err)
	}

	fsrc, err := os.OpenFile("/tmp/src/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}
	defer fsrc.Close()
	if _, err := fsrc.WriteString(src); err != nil {
		t.Fatalf("Failed to write to source file: %v", err)
	}
}

func teardown(t *testing.T) {
	if err := os.RemoveAll("/tmp/src"); err != nil {
		t.Fatalf("Failed to remove source directory: %v", err)
	}
}

func TestNewFileIndex(t *testing.T) {
	cache := NewFileIndex("/tmp", "test.txt", 10)
	if len(cache.hashTable) != 0 {
		t.Errorf("Expected empty cache, got %d items", len(cache.hashTable))
	}
	if cache.fileName != "test.txt" {
		t.Errorf("Expected filename 'testfile.txt', got %s", cache.fileName)
	}
	if cache.blockCount != 0 {
		t.Errorf("Expected block count 0, got %d", cache.blockCount)
	}
}

func Test_LookupHashTable(t *testing.T) {
	setup(t, "This is a test files")
	defer teardown(t)
	cache := NewFileIndex("/tmp/src", "test.txt", 4)
	cache.RegenerateFileIndex()
	hash, ok := cache.LookupHashTable(0)
	if !ok {
		t.Errorf("Expected to find hash for chunk 0")
	}
	if hash != 14873379861052371851 {
		t.Errorf("Expected hash 14873379861052371851, got %d", hash)
	}

	nonExistentHash, ok := cache.LookupHashTable(100)
	if ok {
		t.Errorf("Expected not to find hash for chunk 100, got %d", nonExistentHash)
	}
	if nonExistentHash != 0 {
		t.Errorf("Expected hash 0 for non-existent chunk, got %d", nonExistentHash)
	}
}

func Test_UpdateHashTable(t *testing.T) {
	setup(t, "This is a test files")
	defer teardown(t)
	cache := NewFileIndex("/tmp/src", "test.txt", 4)
	cache.RegenerateFileIndex()
	cache.UpdateChunckHash(0, 123456789)
	hash, ok := cache.LookupHashTable(0)
	if !ok {
		t.Errorf("Expected to find hash for chunk 0")
	}
	if hash != 123456789 {
		t.Errorf("Expected hash 123456789, got %d", hash)
	}

	cache.UpdateChunckHash(5, 987654321)
	hash, ok = cache.LookupHashTable(5)
	if !ok {
		t.Errorf("Expected to find hash for chunk 5")
	}
	if hash != 987654321 {
		t.Errorf("Expected hash 987654321, got %d", hash)
	}
}
