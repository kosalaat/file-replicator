package server

import (
	"context"
	"io"
	"io/fs"
	"net"
	"os"
	"path"
	"syscall"

	"github.com/cespare/xxhash/v2"
	"github.com/kosalaat/file-replicator/replicator"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

var serverlogger = log.With().Str("component", "server").Logger()

type ReplicationServer struct {
	replicator.UnimplementedFileReplicatorServer
	FileRoot string
	Server   *grpc.Server
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

	file, err := os.Open(path.Join(s.FileRoot, in.RelativeFilePath))
	if err != nil {
		if os.IsNotExist(err) {
			serverlogger.Warn().Msgf("File %s does not exist, skipping...", in.RelativeFilePath)
			return &replicator.Confirmation{
				Code:  replicator.ConfirmationCode_CHANGES_REPORTED,
				Chunk: in.Chunk,
			}, nil
		} else {
			serverlogger.Error().Err(err).Msgf("Failed to open file for readonly [%s]", in.RelativeFilePath)
			return &replicator.Confirmation{
				Code: replicator.ConfirmationCode_UNHANDLED_ERROR,
			}, err
		}
	} else {
		defer file.Close()
	}

	serverlogger.Info().Msgf("Checking %d chunks", len(in.Chunk))

	buf := make([]byte, in.BlockSize)

	chunkOut := make([]*replicator.ChunkInfo, 0)

	for _, chunk := range in.Chunk {
		offset, err := file.Seek(int64(uint64(chunk.ChunkID*in.BlockSize)), io.SeekStart)
		if err != nil {
			serverlogger.Warn().Err(err).Msgf("Failed to seek to chunk %d, may be source file is longer", chunk.ChunkID)
			chunkOut = append(chunkOut, chunk)
		}

		n, err := file.Read(buf)
		if err != nil {
			serverlogger.Warn().Err(err).Msgf("Failed to read chunk %d, may be source file is longer", chunk.ChunkID)
			chunkOut = append(chunkOut, chunk)
		} else {
			serverlogger.Info().Msgf("Read %d bytes from file at offset %d", n, offset)
			if n > 0 {
				if chunk.Hash != xxhash.Sum64(buf[:n]) {
					serverlogger.Info().Msgf("Chunk %d is changed, expected hash: %d, actual hash: %d", chunk.ChunkID, chunk.Hash, xxhash.Sum64(buf[:n]))
					chunkOut = append(chunkOut, chunk)
				} else {
					serverlogger.Info().Msgf("Chunk %d is unchanged", chunk.ChunkID)
				}
			} else {
				serverlogger.Info().Msgf("Chunk %d is empty, may be source file is longer", chunk.ChunkID)
				chunkOut = append(chunkOut, chunk)
			}
		}
	}
	serverlogger.Info().Msgf("Found %d changed chunks", len(chunkOut))

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
