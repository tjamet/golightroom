package lightroom

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	// Add support for sqlite databases
	_ "github.com/mattn/go-sqlite3"
)

// FileRef holds both the 'absolute' path and the lightroom FileID for a given file.
type FileRef struct {
	FileID int
	Path   string
}

// FileRefSlice implements basic functions to allow sorting a slice of FileRefs
type FileRefSlice []FileRef

// Len ...
func (s FileRefSlice) Len() int {
	return len(s)
}

// Swap ...
func (s FileRefSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// DuplicateFile holds all detected duplicates of a file, grouped by checksum
type DuplicateFile struct {
	FileRef  FileRefSlice
	Checksum string
}

// OpenDuplicate creates a new connection to a duplicates database.
//
// The database leaves as a sidecar of the main lightroom library.
// If the database does not exist, it will be created with no schema.
func OpenDuplicate(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "file:"+path+"-duplicates?mode=rwc")
	return db, err
}

// InitDuplicate initializes a duplicates database, if the tables already exist, they are deleted and re-created.
func InitDuplicate(db *sql.DB) error {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS
			LightroomFileChecksum`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TABLE
					LightroomFileChecksum (
						id_local INTEGER PRIMARY KEY,
						path NOT NULL DEFAULT '',
						checksum
					);
		CREATE UNIQUE INDEX
					idx_LightroomFileChecksum_path
		ON
					LightroomFileChecksum (path);
		CREATE INDEX
					idx_LightroomFileChecksum_checksum
		ON
					LightroomFileChecksum (checksum);`)
	if err != nil {
		return err
	}

	return nil
}

// InsertDuplicate inserts the lightroom file in the duplicates database.
func InsertDuplicate(db *sql.DB, f File, checksum string) error {
	_, err := db.Exec(`INSERT INTO LightroomFileChecksum (id_local, path, checksum) VALUES (?,?,?)`, f.FileID, f.Path(), checksum)
	return err
}

// ListDuplicateFiles iterates over all files in the duplicates table.
//
// Duplication is detected using the file checksum
// Files returned may contain only one file reference, in such a case, the file is not duplicated.
func ListDuplicateFiles(ctx context.Context, db *sql.DB) (chan DuplicateFile, chan error) {
	const bulkSize = 5000000
	r := make(chan DuplicateFile)
	e := make(chan error, 1)
	go func(ctx context.Context, db *sql.DB, r chan DuplicateFile, e chan error) {
		defer close(r)
		defer close(e)

		rows, err := db.Query(`
				select
						group_concat(id_local) as id_locals,
						group_concat(path) as paths,
						checksum
				from
						LightroomFileChecksum
				group by
						checksum
				;`,
		)
		if err != nil {
			select {
			case e <- err:
			case <-ctx.Done():
			}
			return
		}
		defer rows.Close()
		for rows.Next() {
			f := DuplicateFile{
				FileRef: FileRefSlice{},
			}
			fileIdsSerialized := ""
			pathsSerialized := ""
			err := rows.Scan(&fileIdsSerialized, &pathsSerialized, &f.Checksum)
			if err != nil {
				select {
				case e <- err:
				case <-ctx.Done():
					return
				}
			}
			if fileIdsSerialized != "" && pathsSerialized != "" {
				fileIDs := strings.Split(fileIdsSerialized, ",")
				paths := strings.Split(pathsSerialized, ",")
				if len(fileIDs) != len(paths) {
					select {
					case e <- fmt.Errorf("unconsistent number file IDs and Path"):
					case <-ctx.Done():
						return
					}
				}

				for i, fileIDString := range fileIDs {
					fielID, _ := strconv.Atoi(fileIDString)
					f.FileRef = append(f.FileRef, FileRef{
						FileID: fielID,
						Path:   paths[i],
					})
				}
			}
			select {
			case r <- f:
			case <-ctx.Done():
				return
			}
		}
	}(ctx, db, r, e)
	return r, e
}

// ProcessAllDuplicateFiles walks through all references in the duplicates database, calling one handler per checksum
func ProcessAllDuplicateFiles(db *sql.DB, handler func(DuplicateFile, Counter) error, counter Counter) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	files, DBerrors := ListDuplicateFiles(ctx, db)
	processErrors := processAllDuplicateFilesInChan(files, handler, counter)
	select {
	case err := <-DBerrors:
		return err
	case err := <-processErrors:
		return err
	}
}

func processAllDuplicateFilesInChan(files chan DuplicateFile, handler func(DuplicateFile, Counter) error, counter Counter) chan error {
	errors := make(chan error)
	defer close(errors)
	go func(errChan chan error) {
		count := 0
		for f := range files {
			count++
			err := handler(f, counter)
			if err != nil {
				errChan <- err
			}
		}
	}(errors)
	return errors
}
