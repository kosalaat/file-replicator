package client

import (
	"context"
	"io"
	"os"
	"path"
	"syscall"

	"github.com/cespare/xxhash/v2"
	"github.com/kosalaat/file-replicator/replicator"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var clientlogger = log.With().Str("component", "client").Logger()

type ReplicatorClient struct {
	conn         grpc.ClientConnInterface
	FileRoot     string
	parallelRuns uint64
	replicator.FileReplicatorClient
}

func NewReplicatorClient(address string, fileRoot string, parallelRuns uint64) (*ReplicatorClient, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		clientlogger.Error().Err(err).Msg("Failed to connect to server")
		return nil, err
	}

	client := replicator.NewFileReplicatorClient(conn)
	clientlogger.Info().Msg("Connected to server successfully")
	return &ReplicatorClient{conn: conn, FileReplicatorClient: client, parallelRuns: parallelRuns, FileRoot: fileRoot}, nil
}

func (r *ReplicatorClient) ReplicateChunk(ctx context.Context, chunk *replicator.DataPayload) (*replicator.Confirmation, error) {
	clientlogger.Info().Msg("Sending chunk to server...")
	confirmation, err := r.FileReplicatorClient.Replicate(ctx, chunk)
	if err != nil {
		clientlogger.Error().Err(err).Msg("Failed to send chunk")
		return nil, err
	}
	clientlogger.Info().Msg("Chunk sent successfully")
	return confirmation, nil
}

func (r *ReplicatorClient) CheckDuplicates(
	ctx context.Context,
	file string,
	blockSize uint64,
) (*replicator.Confirmation, error) {
	clientlogger.Info().Msg("Checking for duplicates...")

	fileInfo, err := os.Open(path.Join(r.FileRoot, file))
	if err != nil {
		clientlogger.Error().Err(err).Msg("Failed to get file info")
		return nil, err
	}

	defer fileInfo.Close()

	buf := make([]byte, blockSize)

	fileStat, err := fileInfo.Stat()
	if err != nil {
		clientlogger.Error().Err(err).Msg("Failed to get file size")
		return nil, err
	}

	stat, _ := fileStat.Sys().(*syscall.Stat_t)
	if stat == nil {
		clientlogger.Error().Msg("Failed to get file ownership information")
		return nil, os.ErrInvalid
	}

	response := &replicator.DataSignature{
		RelativeFilePath: file,
		BlockSize:        uint64(blockSize),
		FileSize:         uint64(fileStat.Size()),
		FileMode:         uint32(fileStat.Mode()),
		UID:              uint32(stat.Uid),
		GID:              uint32(stat.Gid),
	}

	var blockId uint64

	for blockId = 0; ; blockId++ {

		n, err := fileInfo.Read(buf)
		if err != nil && err != io.EOF {
			clientlogger.Error().Err(err).Msg("Failed to read file")
			return &replicator.Confirmation{
				Code: replicator.ConfirmationCode_FILE_NOT_READABLE,
			}, err
		}
		clientlogger.Info().Msgf("Read %d bytes from file, data: %s", n, string(buf[:n]))

		if err == io.EOF {
			if n == 0 {
				clientlogger.Info().Msg("No more data to read from file")
			} else {
				clientlogger.Info().Msg("End of file reached, processing last chunk")
				chunk := replicator.ChunkInfo{
					Hash:      xxhash.Sum64(buf[:n]),
					BlockSize: uint64(n),
					ChunkID:   blockId,
				}
				response.Chunk = append(response.Chunk, &chunk)
			}
			break
		}
		if n > 0 {
			clientlogger.Info().Msgf("Read chunk %d", blockId)
			chunk := replicator.ChunkInfo{
				Hash:      xxhash.Sum64(buf[:n]),
				BlockSize: uint64(n),
				ChunkID:   blockId,
			}
			response.Chunk = append(response.Chunk, &chunk)
		}
	}

	clientlogger.Info().Msgf("Sending %d chunks to server", len(response.Chunk))

	confirmation, err := r.FileReplicatorClient.CheckDuplicates(
		ctx,
		response,
		grpc.WaitForReady(true),
	)
	if err != nil {
		clientlogger.Error().Err(err).Msg("Failed to check duplicates")
		return nil, err
	}
	clientlogger.Info().Msgf("Duplicate check completed successfully, found changes: %d", len(confirmation.Chunk))
	return confirmation, nil
}

func (r *ReplicatorClient) RenameFile(ctx context.Context, oldPath, newPath string) (*replicator.Confirmation, error) {
	clientlogger.Info().Msgf("Renaming file from %s to %s", oldPath, newPath)
	confirmation, err := r.FileReplicatorClient.Rename(
		ctx,
		&replicator.FileOps{
			RelativeFilePath:    oldPath,
			NewRelativeFilePath: newPath,
		},
	)
	if err != nil {
		clientlogger.Error().Err(err).Msg("Failed to rename file")
		return confirmation, err
	}
	clientlogger.Info().Msg("File renamed successfully")
	return confirmation, nil
}

func (r *ReplicatorClient) DeleteFile(ctx context.Context, filePath string) (*replicator.Confirmation, error) {
	clientlogger.Info().Msgf("Deleting file: %s", filePath)
	confirmation, err := r.FileReplicatorClient.Delete(
		ctx,
		&replicator.FileOps{
			RelativeFilePath: filePath,
		},
	)
	if err != nil {
		clientlogger.Error().Err(err).Msg("Failed to delete file")
		return confirmation, err
	}
	clientlogger.Info().Msgf("File deleted successfully. Confirmation code is: %s", confirmation.Code)
	return confirmation, nil
}

func (r *ReplicatorClient) Ping(ctx context.Context, in *replicator.PingPong) *replicator.PingPong {
	pong, err := r.FileReplicatorClient.Ping(ctx, in)
	if err != nil {
		clientlogger.Error().Err(err).Msg("Failed to ping server")
		return nil
	} else {
		return pong
	}
}
