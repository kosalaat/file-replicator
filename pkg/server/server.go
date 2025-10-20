package server

import (
	"context"
	"io/fs"
	"net"
	"os"
	"path"
	"syscall"

	"github.com/kosalaat/file-replicator/pkg/controller"
	"github.com/kosalaat/file-replicator/replicator"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

var serverlogger = log.With().Str("component", "server").Logger()

type ReplicationServer struct {
	replicator.UnimplementedFileReplicatorServer
	FileRoot string
	hashMap  map[string]controller.FileIndex
	Server   *grpc.Server
}

func NewReplicationServer() *ReplicationServer {
	serverlogger.Info().Msg("Creating new ReplicationServer instance")
	return &ReplicationServer{
		hashMap: make(map[string]controller.FileIndex),
	}
}

func (r *ReplicationServer) StartListening(address string, FileRoot string) error {
	server := grpc.NewServer()
	r.Server = server

	listener, err := net.Listen("tcp", address)
	if err != nil {
		serverlogger.Error().Err(err).Msgf("Failed to listen on %s", address)
		panic(err)
	} else {
		defer listener.Close()
	}
	serverlogger.Info().Msgf("Listening on %s...", address)

	replicator.RegisterFileReplicatorServer(server, r)

	r.FileRoot = FileRoot
	serverlogger.Info().Msg("Ready to recieve files updates...")

	err = server.Serve(listener)
	if err != nil {
		serverlogger.Error().Err(err).Msg("Failed to serve gRPC server")
		return err
	} else {
		serverlogger.Info().Msg("gRPC Server stopped gracefully")
	}
	return nil
}

func (r *ReplicationServer) StopListening() {
	if r.Server != nil {
		serverlogger.Info().Msg("gRPC Server Stopping...")
		r.Server.GracefulStop()
		serverlogger.Info().Msg("gRPC Server stopped")
	} else {
		serverlogger.Warn().Msg("No gRPC Server to stop")
	}
}

func (s *ReplicationServer) CheckDuplicates(ctx context.Context, in *replicator.DataSignature) (*replicator.Confirmation, error) {
	serverlogger.Info().Msg("Calculating changed blocks...")

	chunkOut := make([]*replicator.ChunkInfo, 0)

	for _, chunk := range in.Chunk {
		if _, exists := s.hashMap[in.RelativeFilePath]; !exists {
			serverlogger.Info().Msgf("File index for %s does not exist, creating new index", in.RelativeFilePath)
			fIndex := controller.NewFileIndex(s.FileRoot, in.RelativeFilePath, in.BlockSize)
			if err := fIndex.RegenerateFileIndex(); err != nil {
				serverlogger.Error().Err(err).Msgf("Failed to regenerate file index for %s", in.RelativeFilePath)
				return &replicator.Confirmation{
					Code: replicator.ConfirmationCode_UNHANDLED_ERROR,
				}, err
			}
			serverlogger.Info().Msgf("Type %T, %v", s.hashMap, s.hashMap)
			s.hashMap[in.RelativeFilePath] = fIndex
		} else {
			serverlogger.Info().Msgf("File index for %s exists, using cached index", in.RelativeFilePath)
		}

		fIndex := s.hashMap[in.RelativeFilePath]
		cHash, ok := fIndex.LookupHashTable(chunk.ChunkID)
		if ok {
			if cHash == chunk.Hash {
				serverlogger.Info().Msgf("Chunk %d is unchanged (from cache)", chunk.ChunkID)
				continue
			} else {
				serverlogger.Info().Msgf("Chunk %d is changed (from cache), expected hash: %d, actual hash: %d", chunk.ChunkID, chunk.Hash, cHash)
				chunkOut = append(chunkOut, chunk)
			}
		} else {
			serverlogger.Info().Msgf("Chunk %d not found in cache, regenerating file index", chunk.ChunkID)
			chunkOut = append(chunkOut, chunk)
		}
	}

	return &replicator.Confirmation{
		Code: func() replicator.ConfirmationCode {
			if len(chunkOut) > 0 {
				return replicator.ConfirmationCode_CHANGES_REPORTED
			} else {
				return replicator.ConfirmationCode_CHANGES_NOT_FOUND
			}
		}(),
		Chunk: chunkOut,
	}, nil
}

func (s *ReplicationServer) Replicate(ctx context.Context, in *replicator.DataPayload) (*replicator.Confirmation, error) {

	// Implement the replication logic here
	// For example, save the file to a specific location
	outFile, err := os.OpenFile(
		path.Join(s.FileRoot, in.RelativeFilePath),
		os.O_WRONLY|os.O_CREATE,
		os.FileMode(in.FileMode),
	)
	if err != nil {
		log.Error().Err(err).Msg("Failed to open for writing")
		return &replicator.Confirmation{
			Code: replicator.ConfirmationCode_FILE_NOT_WRITABLE,
		}, err
	} else {
		defer outFile.Close()
		if outStat, _ := outFile.Stat(); outStat.Size() > int64(in.FileSize) {
			log.Info().Msgf("File size: %d, larger than expected: %d, truncating file", outStat.Size(), in.FileSize)
			err = outFile.Truncate(int64(in.FileSize))
			if err != nil {
				log.Error().Err(err).Msg("Failed to truncate file")
				return &replicator.Confirmation{
					Code: replicator.ConfirmationCode_FILE_NOT_WRITABLE,
				}, err
			}
		}
	}
	log.Info().Msgf("Replicating file %s...", outFile.Name())

	if in.DataChunk != nil {
		offset := in.BlockSize * in.ChunkID
		if outStat, _ := outFile.Stat(); outStat.Size() < int64(offset) {
			log.Info().Msgf("File size: %d, smaller than offset: %d, truncating file", outStat.Size(), offset)
			err = outFile.Truncate(int64(offset))
			if err != nil {
				log.Error().Err(err).Msg("Failed to truncate file")
				return &replicator.Confirmation{
					Code: replicator.ConfirmationCode_FILE_NOT_WRITABLE,
				}, err
			}
		}
		outFile.Seek(int64(offset), 0)

		_, err = outFile.Write(in.DataChunk)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to write data chunk: %d", in.ChunkID)
			return &replicator.Confirmation{
				Code: replicator.ConfirmationCode_UPDATE_ERROR,
			}, err
		}
		log.Info().Msgf("Wrote chunk %d of size %d", in.ChunkID, len(in.DataChunk))
		return &replicator.Confirmation{
			Code: replicator.ConfirmationCode_OK,
		}, nil
	} else {
		log.Info().Msg("Processing owner/access change.")
		outStat, err := os.Stat(outFile.Name())
		if err != nil {
			log.Error().Err(err).Msg("Failed to stat file")
			return &replicator.Confirmation{
				Code: replicator.ConfirmationCode_FILE_NOT_READABLE,
			}, err
		} else {
			if outStat.Mode() == fs.FileMode(in.FileMode) {
				log.Info().Msg("File mode is already set, skipping...")
			} else {
				log.Info().Msgf("Setting file mode to %o", in.FileMode)
				if err := outFile.Chmod(os.FileMode(in.FileMode)); err != nil {
					log.Error().Err(err).Msg("Failed to change file mode")
					return &replicator.Confirmation{
						Code: replicator.ConfirmationCode_FILE_NOT_WRITABLE,
					}, err
				}
			}
			if stat, _ := outStat.Sys().(*syscall.Stat_t); in.UID != stat.Uid || in.GID != stat.Gid {
				log.Info().Msgf("Setting file ownership to UID: %d, GID: %d", in.UID, in.GID)
				if err := os.Chown(outFile.Name(), int(in.UID), int(in.GID)); err != nil {
					log.Error().Err(err).Msg("Failed to change file ownership")
					return &replicator.Confirmation{
						Code: replicator.ConfirmationCode_FILE_NOT_WRITABLE,
					}, err
				}
			} else {
				log.Info().Msg("No ownership change requested, skipping...")
			}
		}
		return &replicator.Confirmation{
			Code: replicator.ConfirmationCode_OK,
		}, nil
	}
}

func (s *ReplicationServer) Rename(ctx context.Context, in *replicator.FileOps) (*replicator.Confirmation, error) {
	oldPath := path.Join(s.FileRoot, in.RelativeFilePath)
	newPath := path.Join(s.FileRoot, in.NewRelativeFilePath)

	serverlogger.Info().Msgf("Renaming file from %s to %s", oldPath, newPath)

	if err := os.Rename(oldPath, newPath); err != nil {
		serverlogger.Error().Err(err).Msg("Failed to rename file")
		return &replicator.Confirmation{
			Code: replicator.ConfirmationCode_UPDATE_ERROR,
		}, err
	}

	return &replicator.Confirmation{
		Code: replicator.ConfirmationCode_OK,
	}, nil
}

func (s *ReplicationServer) Delete(ctx context.Context, in *replicator.FileOps) (*replicator.Confirmation, error) {
	filePath := path.Join(s.FileRoot, in.RelativeFilePath)
	archiveFolder := path.Join(s.FileRoot, ".archive")

	serverlogger.Info().Msgf("Deleting file %s", filePath)

	if _, err := os.Stat(archiveFolder); os.IsNotExist(err) {
		serverlogger.Info().Msgf("Archive folder %s does not exist, creating...", archiveFolder)
		if err := os.Mkdir(archiveFolder, 0755); err != nil {
			serverlogger.Error().Err(err).Msgf("Failed to create archive folder %s", archiveFolder)
			return &replicator.Confirmation{
				Code: replicator.ConfirmationCode_UPDATE_ERROR,
			}, err
		}
	}
	destPath := path.Join(archiveFolder, in.RelativeFilePath)
	serverlogger.Info().Msgf("Creating the destination folder structure: %s", path.Dir(destPath))
	if err := os.MkdirAll(path.Dir(destPath), 0750); err != nil {
		serverlogger.Error().Err(err).Msgf("Failed to create destination archive directory %s", path.Dir(destPath))
		return &replicator.Confirmation{
			Code: replicator.ConfirmationCode_UPDATE_ERROR,
		}, err
	}

	serverlogger.Info().Msgf("Moving file %s to archive %s", filePath, destPath)
	if err := os.Rename(filePath, destPath); err != nil {
		if os.IsNotExist(err) {
			serverlogger.Warn().Msgf("File %s does not exist, nothing to delete", filePath)
			return &replicator.Confirmation{
				Code: replicator.ConfirmationCode_FILE_NOT_FOUND,
			}, nil
		}
		serverlogger.Error().Err(err).Msg("Failed to delete file")
		return &replicator.Confirmation{
			Code: replicator.ConfirmationCode_UPDATE_ERROR,
		}, err
	}

	return &replicator.Confirmation{
		Code: replicator.ConfirmationCode_OK,
	}, nil
}

func (s *ReplicationServer) Ping(ctx context.Context, in *replicator.PingPong) (*replicator.PingPong, error) {
	return &replicator.PingPong{
		Val: in.Val,
	}, nil
}
