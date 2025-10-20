package controller

import (
	"io"
	"os"
	"path"

	"github.com/cespare/xxhash/v2"
	"github.com/rs/zerolog/log"
)

var cacheLogger = log.With().Str("component", "cache-engine").Logger()

type FileIndex struct {
	fileRoot   string
	fileName   string
	blockCount uint64
	blockSize  uint64
	hashTable  []uint64
}

func NewFileIndex(fileRoot string, fileName string, blockSize uint64) FileIndex {
	return FileIndex{
		fileRoot:   fileRoot,
		fileName:   fileName,
		blockSize:  blockSize,
		blockCount: 0,
		hashTable:  []uint64{},
	}
}

func (f *FileIndex) LookupHashTable(chunkId uint64) (uint64, bool) {
	if chunkId < uint64(len(f.hashTable)) {
		return f.hashTable[chunkId], true
	} else {
		return 0, false
	}
}

func (f *FileIndex) RegenerateFileIndex() error {
	fileHandler, err := os.Open(path.Join(f.fileRoot, f.fileName))
	if err != nil {
		cacheLogger.Error().Err(err).Msg("Failed to open file")
		return err
	}
	defer fileHandler.Close()

	buffer := make([]byte, f.blockSize)

	for chunkId := 0; ; chunkId++ {
		n, err := fileHandler.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			cacheLogger.Error().Err(err).Msg("Failed to read file")
			return err
		}
		if n == 0 {
			break
		} else {
			cacheLogger.Info().Msgf("Read %d bytes from file", n)
			hash := xxhash.Sum64String(string(buffer))
			f.hashTable = append(f.hashTable, hash)
			f.blockCount++
		}
	}
	return nil
}

func (f *FileIndex) UpdateChunckHash(chunkId uint64, hash uint64) {
	if chunkId < uint64(len(f.hashTable)) {
		f.hashTable[chunkId] = hash
	} else if chunkId == uint64(len(f.hashTable)) {
		f.hashTable = append(f.hashTable, hash)
		f.blockCount++
	} else {
		emptybuffer := make([]uint64, chunkId-uint64(len(f.hashTable)))
		f.hashTable = append(f.hashTable, emptybuffer...)
		f.hashTable = make([]uint64, hash)
		cacheLogger.Info().Msgf("Extended hash table to %d entries", len(f.hashTable))
	}
}
