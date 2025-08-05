package files

import (
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
)

var fnotifylogger = log.With().Str("component", "file-notify").Logger()

func (f *FileReplicator) SetupFileWatcher(fileRoot string, blockSize uint64) error {
	f.FileRoot = fileRoot

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
