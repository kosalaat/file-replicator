package files

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/kosalaat/file-replicator/pkg/client"
	"github.com/kosalaat/file-replicator/replicator"
	"github.com/rs/zerolog/log"
)

type FileReplicator struct {
	client.ReplicatorClient
	transferQueue chan *replicator.DataPayload
	watcher       *fsnotify.Watcher
}

var fopslogger = log.With().Str("component", "file-ops").Logger()

func (f *FileReplicator) ProcessFile(file string, blockSize uint64) error {
	fopslogger.Info().Msgf("Processing file: %s", file)

	// define the context with a timeout
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	change, err := f.ReplicatorClient.CheckDuplicates(ctx, file, blockSize)

	if err != nil {
		fopslogger.Error().Err(err).Msg("Failed to check for duplicates")
		return err
	} else {
		fopslogger.Info().Msgf("Change count: %v", len(change.Chunk))
	}

	fileHandle, err := os.Open(path.Join(f.ReplicatorClient.FileRoot, file))
	if err != nil {
		fopslogger.Error().Err(err).Msgf("Failed to open file: %s", file)
		return err
	}
	defer fileHandle.Close()

	// if len(change.Chunk) > 0 {
	for _, chunk := range change.Chunk {
		fopslogger.Info().Msgf("Processing chunk: %d", chunk.ChunkID)

		_, err := fileHandle.Seek(int64(chunk.ChunkID*blockSize), io.SeekStart)
		if err != nil {
			fopslogger.Error().Err(err).Msgf("Failed to seek to chunk: %d", chunk.ChunkID)
			return err
		}
		buf := make([]byte, blockSize)
		n, err := fileHandle.Read(buf)
		fopslogger.Info().Msgf("Read %d bytes data: %s", n, string(buf[:n]))

		if err == io.EOF {
			fopslogger.Warn().Msgf("Reached EOF while reading chunk: %d", chunk.ChunkID)
			if n == 0 {
				fopslogger.Warn().Msgf("Chunk %d is empty, may be source file is longer", chunk.ChunkID)
			}
		} else if err != nil {
			fopslogger.Error().Err(err).Msgf("Failed to read chunk: %d", chunk.ChunkID)
			return err
		}

		fopslogger.Info().Msgf("Read chunk %d with size %d", chunk.ChunkID, n)

		fileStat, _ := fileHandle.Stat()

		f.transferQueue <- &replicator.DataPayload{
			DataChunk:        buf[:n],
			ChunkID:          chunk.ChunkID,
			BlockSize:        uint64(blockSize),
			FileMode:         uint32(fileStat.Mode()),
			FileSize:         uint64(fileStat.Size()),
			Length:           uint64(blockSize),
			UID:              uint32(fileStat.Sys().(*syscall.Stat_t).Uid),
			GID:              uint32(fileStat.Sys().(*syscall.Stat_t).Gid),
			RelativeFilePath: file,
		}

		// if confirmation, err := f.ReplicatorClient.ReplicateChunk(
		// 	ctx,
		// 	&replicator.DataPayload{
		// 		DataChunk:        buf[:n],
		// 		ChunkID:          chunk.ChunkID,
		// 		BlockSize:        uint64(blockSize),
		// 		FileMode:         uint32(fileStat.Mode()),
		// 		FileSize:         uint64(fileStat.Size()),
		// 		Length:           uint64(blockSize),
		// 		UID:              uint32(fileStat.Sys().(*syscall.Stat_t).Uid),
		// 		GID:              uint32(fileStat.Sys().(*syscall.Stat_t).Gid),
		// 		RelativeFilePath: file,
		// 	},
		// ); err != nil {
		// 	fopslogger.Error().Err(err).Msg("Failed to replicate chunk")
		// 	return err
		// } else if confirmation.Code != replicator.ConfirmationCode_OK {
		// 	fopslogger.Error().Msgf("Replication failed with code: %s", confirmation.Code)
		// }
		fopslogger.Info().Msgf("Chunk %d replicated successfully", chunk.ChunkID)
	}
	// } else {
	// 	log.Info().Msg("No chunks to replicate, processing ownership/access change.")
	// }

	return nil
}

func (f *FileReplicator) UpdateOwnership(relativePath string) error {
	// define the context with a timeout
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	stat, err := os.Stat(path.Join(f.FileRoot, relativePath))
	if err != nil {
		fopslogger.Error().Err(err).Msgf("Failed to stat file root: %s", relativePath)
		return err
	}

	mode := stat.Mode()
	UID := uint32(stat.Sys().(*syscall.Stat_t).Uid)
	GID := uint32(stat.Sys().(*syscall.Stat_t).Gid)

	if confirmation, err := f.ReplicatorClient.ReplicateChunk(
		ctx,
		&replicator.DataPayload{
			DataChunk:        nil,
			FileMode:         uint32(mode),
			UID:              UID,
			GID:              GID,
			RelativeFilePath: relativePath,
		},
	); err != nil {
		fopslogger.Error().Err(err).Msg("Failed to replicate ownership change")
		return err
	} else if confirmation.Code != replicator.ConfirmationCode_OK {
		fopslogger.Error().Msgf("Ownership change failed with code: %s", confirmation.Code)
		return errors.New("Ownership change failed")
	}
	return nil
}

func (f *FileReplicator) RenameFile(relativePath string, newRelativePath string) error {
	// define the context with a timeout
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	if confirmation, err := f.ReplicatorClient.RenameFile(
		ctx,
		relativePath,
		newRelativePath,
	); err != nil {
		fopslogger.Error().Err(err).Msg("Failed to rename file")
		return err
	} else if confirmation.Code != replicator.ConfirmationCode_OK {
		fopslogger.Error().Msgf("Rename failed with code: %s", confirmation.Code)
		return errors.New("rename failed")
	}
	return nil
}

func (f *FileReplicator) DeleteFile(relativePath string) error {
	// define the context with a timeout
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	if confirmation, err := f.ReplicatorClient.DeleteFile(
		ctx,
		relativePath,
	); err != nil {
		fopslogger.Error().Err(err).Msg("Failed to delete file")
		return err
	} else if confirmation.Code != replicator.ConfirmationCode_OK {
		fopslogger.Error().Msgf("Delete failed with code: %s", confirmation.Code)
		return errors.New("delete failed")
	}
	return nil
}
