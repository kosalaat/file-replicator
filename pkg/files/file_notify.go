package files

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/kosalaat/file-replicator/replicator"
	"github.com/rs/zerolog/log"
)

var fnotifylogger = log.With().Str("component", "file-notify").Logger()

func (f *FileReplicator) SyncSource(fileRoot string, blockSize uint64) error {
	fnotifylogger.Info().Msgf("Starting initial sync for directory: %s", fileRoot)

	err := filepath.Walk(
		fileRoot,
		func(path string, info os.FileInfo, err error) error {
			referencePath, _ := filepath.Rel(fileRoot, path)
			if !info.IsDir() {
				f.ProcessFile(referencePath, blockSize)
			}
			return nil
		},
	)
	if err != nil {
		fnotifylogger.Error().Err(err).Msgf("Failed to walk directory: %s", fileRoot)
	}
	return nil
}

func (f *FileReplicator) SetupFileWatcher(fileRoot string, blockSize uint64) error {
	f.FileRoot = fileRoot
	f.transferQueue = make(chan *replicator.DataPayload, 1000)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	} else {
		f.watcher = watcher
	}

	err = f.watcher.Add(fileRoot)
	if err != nil {
		return err
	}

	//Start the transferQueue reader
	go func(chan *replicator.DataPayload) {
		var dataPayload *replicator.DataPayload
		// ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(time.Millisecond*100))
		// defer cancelFunc()
		ctx := context.Background()

		for {
			select {
			case dataPayload = <-f.transferQueue:
				if _, err := f.ReplicatorClient.ReplicateChunk(ctx, dataPayload); err != nil {
					fnotifylogger.Error().Err(err).Msgf("Failed to replicate chunk: %d", dataPayload.ChunkID)
				} else {
					fnotifylogger.Info().Msgf("Successfully replicated chunk: %d", dataPayload.ChunkID)
				}
				// Handle value from ch1
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}(f.transferQueue)

	// Scan for the initial sync

	go func(fileRoot string, blockSize uint64) {
		fnotifylogger.Info().Msgf("Starting initial sync for directory: %s", fileRoot)

		err := filepath.Walk(
			fileRoot,
			func(path string, info os.FileInfo, err error) error {
				referencePath, _ := filepath.Rel(fileRoot, path)
				if !info.IsDir() {
					f.ProcessFile(referencePath, blockSize)
				}
				return nil
			},
		)
		if err != nil {
			fnotifylogger.Error().Err(err).Msgf("Failed to walk directory: %s", fileRoot)
		}
	}(f.FileRoot, blockSize)

	// go func() {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			fileName, err := filepath.Rel(fileRoot, event.Name)
			if err != nil {
				fnotifylogger.Error().Err(err).Msgf("Failed to get relative path for event: %s", event.Name)
			}
			fnotifylogger.Info().Msgf("File event: %s, Operation: %s", fileName, event.Op)
			switch event.Op {
			case fsnotify.Create, fsnotify.Write:
				go func(fileName string, blockSize uint64) {
					if f.ProcessFile(fileName, blockSize) != nil {
						fnotifylogger.Info().Msgf("Failed to process file: %s", fileName)

					}
				}(fileName, blockSize)
			case fsnotify.Chmod:
				go func(fileName string) {
					if f.UpdateOwnership(fileName) != nil {
						fnotifylogger.Info().Msgf("Failed to update permissions for file: %s", fileName)
					}
				}(fileName)
			case fsnotify.Remove:
				go func(fileName string) {
					if f.DeleteFile(fileName) != nil {
						fnotifylogger.Info().Msgf("Failed to remove file: %s", fileName)
					} else {
						fnotifylogger.Info().Msgf("File removed: %s", fileName)
					}
				}(fileName)
				// case fsnotify.Rename:
				// 	if f.RenameFile(event.Name, event.) != nil {
				// 		log.Info().Msgf("Failed to rename file: %s", event.Name)
				// 	} else {
				// 		log.Info().Msgf("File renamed: %s", event.Name)
				// 	}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			if err != nil {
				continue
			}
		}
	}
	// }()

}
