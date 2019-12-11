package lightroom

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Add support for sqlite databases. Lightroom catalog is a sqlite database
	_ "github.com/mattn/go-sqlite3"
)

// Counter allows to count items for processors to report progress
type Counter interface {
	Inc(metric string)
}

// NopCounter implements the Counter interface with no tracked action
type NopCounter struct{}

// Inc does nothing as a dummy Counter
func (NopCounter) Inc(string) {}

// Error tracking an error without loosing track of the parent error, and types
type Error struct {
	Type        string
	Message     string
	ParentError error
}

func (e Error) Error() string {
	if e.ParentError != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.ParentError.Error())
	}
	return e.Message
}

// File holds the data gathered from Lightroom database about a given library file.
//
// A File can be either a picture or a video.
type File struct {
	// AbsolutePath holds the path of the root directory in lightroom. It is, from observations, either absolute or relative from the directory the catalog lives in.
	AbsolutePath      string
	PathFromRoot      string
	Filename          string
	Basename          string
	SidecarExtensions []string
	RootFolderID      int
	FolderID          int
	FileID            int
}

// Path returns the full path of a given file on the file system.
//
// In case the path is relative, it is, from observations, provided relative from the directory the catalog lives in.
func (f *File) Path() string {
	return filepath.Join(f.AbsolutePath, f.PathFromRoot, f.Filename)
}

// FindSidecarsExtensions returns the list of all extensions a file from disk
func (f *File) FindSidecarsExtensions() ([]string, error) {
	_, err := os.Stat(f.Path())
	if os.IsNotExist(err) {
		return []string{}, err
	}

	retries := 0
	delay := 5 * time.Nanosecond
	const maxDelay = 5 * time.Millisecond
	const maxRetries = 5
	var dir *os.File
	for {
		dir, err = os.Open(filepath.Join(f.AbsolutePath, f.PathFromRoot))
		if err != nil && retries < maxRetries {
			time.Sleep(delay)
			retries++
			if delay < maxDelay {
				delay *= 2
			}
			continue
		}
		break
	}

	if err != nil {
		return nil, err
	}
	defer dir.Close()
	for {
		paths := []string{}
		names, err := dir.Readdirnames(-1)
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			if strings.HasPrefix(n, f.Basename) {
				paths = append(paths, filepath.Join(f.AbsolutePath, f.PathFromRoot, n))
			}
		}
		if err != nil {
			return nil, err
		}
		baseExt := filepath.Ext(f.Filename)
		if len(baseExt) > 0 {
			baseExt = baseExt[1:]
		}
		return mergeExtensions(baseExt, f.SidecarExtensions, paths), nil
	}
}

// Open creates a new connection to a lightroom catalog.
//
// The database is opened read/write for update purposes.
func Open(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "file:"+path+"?mode=rw")
	return db, err
}

// Close cleanly closes the connection to a catalog.
//
// It is recommended to be called in a defer right after a connection is created:
// ```
//		db, err := lightroom.Open("path-to.lrcat")
//		if err != nil {
//			// do something
//		}
//		defer lightroom.Close(db)
// ```
func Close(db *sql.DB) error {
	return db.Close()
}

// CountFiles returns the number of files referenced in a lightroom catalog
func CountFiles(db *sql.DB) (int, error) {
	rows, err := db.Query("select count(*) from aglibraryfile")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		r := 0
		rows.Scan(&r)
		return r, nil
	}
	return 0, errors.New("could not count files in database")
}

// FindFile retrieves a File details from a lightroom catalog, given its ID
func FindFile(db *sql.DB, fileID int) (File, error) {
	rows, err := db.Query(`
		select 
				absolutePath,
				pathFromRoot,
				basename,
				originalFilename,
				sidecarExtensions,
				folder.rootFolder,
				file.folder,
				file.id_local
		from
				aglibraryfile as file
		join
				aglibraryfolder as folder
		on
				file.folder=folder.id_local
		join
				aglibraryrootfolder as rfolder
		on
				folder.rootFolder=rfolder.id_local
		where
				file.id_local = ?
		;`,
		fileID,
	)
	if err != nil {
		return File{}, err
	}
	defer rows.Close()
	for rows.Next() {
		f := File{}
		sidecarExtensionsSerialized := ""
		err := rows.Scan(&f.AbsolutePath, &f.PathFromRoot, &f.Basename, &f.Filename, &sidecarExtensionsSerialized, &f.RootFolderID, &f.FolderID, &f.FileID)
		if err != nil {
			return File{}, err
		}
		if sidecarExtensionsSerialized != "" {
			sidecarExtensions := strings.Split(sidecarExtensionsSerialized, ",")
			f.SidecarExtensions = sidecarExtensions[:]
		} else {
			f.SidecarExtensions = []string{}
		}
		return f, nil
	}
	return File{}, fmt.Errorf("failed to find file with id %d", fileID)
}

// ListFiles allows to iterate over all known files in a lightroom catalog
func ListFiles(ctx context.Context, db *sql.DB) (chan File, chan error) {
	const bulkSize = 5000000
	r := make(chan File)
	e := make(chan error, 1)
	go func(ctx context.Context, db *sql.DB, r chan File, e chan error) {
		defer close(r)
		defer close(e)

		rows, err := db.QueryContext(ctx, `
				select 
						absolutePath,
						pathFromRoot,
						basename,
						originalFilename,
						sidecarExtensions,
						folder.rootFolder,
						file.folder,
						file.id_local
				from
						aglibraryfile as file
				join
						aglibraryfolder as folder
				on
						file.folder=folder.id_local
				join
						aglibraryrootfolder as rfolder
				on
						folder.rootFolder=rfolder.id_local
				;`,
		)
		if err != nil {
			select {
			case <-ctx.Done():
			case e <- err:
			}
			return
		}
		defer rows.Close()
		for rows.Next() {
			f := File{}
			sidecarExtensionsSerialized := ""
			err := rows.Scan(&f.AbsolutePath, &f.PathFromRoot, &f.Basename, &f.Filename, &sidecarExtensionsSerialized, &f.RootFolderID, &f.FolderID, &f.FileID)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case e <- err:
				}
			}
			if sidecarExtensionsSerialized != "" {
				sidecarExtensions := strings.Split(sidecarExtensionsSerialized, ",")
				f.SidecarExtensions = sidecarExtensions[:]
			} else {
				f.SidecarExtensions = []string{}
			}
			select {
			case <-ctx.Done():
				return
			case r <- f:
			}
		}
	}(ctx, db, r, e)
	return r, e
}

// UpdateSidecarExtensions updates the lightroom catalog wiith the new extension list if needed
func UpdateSidecarExtensions(db *sql.DB, f File, extensions []string) (bool, error) {
	if !sameSlices(extensions, f.SidecarExtensions) {
		sqlStmt, err := db.Prepare(`UPDATE aglibraryfile set sidecarExtensions=? where id_local=?`)

		result, err := sqlStmt.Query(strings.Join(extensions, ","), f.FileID)
		if err == nil {
			result.Next()
			f.SidecarExtensions = extensions[:]
			return true, result.Err()
		}
		result.Close()
		return false, err
	}
	return false, nil
}

// HasAlternative checks whether there exists another file with the same name
// but another extension in the same folder in the lightroom catalog
func HasAlternative(db *sql.DB, f File) (bool, error) {
	r, err := db.Query(fmt.Sprintf(`select count(*) from aglibraryfile where folder = %d and basename = '%s'`, f.FolderID, f.Basename))
	if err != nil {
		return false, err
	}
	defer r.Close()
	for r.Next() {
		count := 0
		err = r.Scan(&count)
		if err != nil {
			return false, err
		}
		return count > 1, nil
	}
	return false, nil
}

// SynchronizeSidecars detects the existing sidecars on disk and ensures the lightroom catalog is up to date with those.
func SynchronizeSidecars(db *sql.DB, f File) *Error {
	sidecarExtensions, err := f.FindSidecarsExtensions()
	if err == nil {
		if len(sidecarExtensions) > 0 {
			if ok, _ := HasAlternative(db, f); ok {

				return nil
			}
			ok, err := UpdateSidecarExtensions(db, f, sidecarExtensions)
			if err != nil {
				return &Error{
					Type:        "update failed",
					Message:     fmt.Sprintf("failed to update sidecars for file %s (id: %d)", f.Path(), f.FileID),
					ParentError: err,
				}
			} else if ok {
				return nil
			}
		}
		return nil
	}
	return &Error{
		Type:        "failed sidecars",
		Message:     fmt.Sprintf("failed get sidecars for file %s (id: %d)", f.Path(), f.FileID),
		ParentError: err,
	}
}

// NewSynchronizeSidecarsHandler returns a handler to be used together with the file walker to synchronize all sidecar of known files in the lightroom catalog
func NewSynchronizeSidecarsHandler(db *sql.DB) func(f File, counter Counter) error {
	return func(f File, counter Counter) error {
		err := SynchronizeSidecars(db, f)
		if err != nil {
			counter.Inc(err.Type)
		} else {
			counter.Inc("updates")
		}
		return err
	}
}

// NewInsertDuplicateHandler allows to walk through all fiiles in the lightroom library and insert its checksum as duplicates in a duplicates database.
func NewInsertDuplicateHandler(duplicateDB *sql.DB, errorLog io.Writer, checksum func(path string) (string, error)) func(f File, counter Counter) error {
	return func(f File, counter Counter) error {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(errorLog, "panic while handling file %s (%d): %v\n", f.Path(), f.FileID, r)
			}
		}()
		c, err := checksum(f.Path())
		if err != nil {
			counter.Inc("checksum failed")
			return fmt.Errorf("failed to compute checksum for file %s (%d): %v", f.Path(), f.FileID, err)
		}
		if err := InsertDuplicate(duplicateDB, f, c); err != nil {
			counter.Inc("insert failed")
			return fmt.Errorf("failed to insert file %s (%d): %v", f.Path(), f.FileID, err)
		}
		return nil
	}
}

// WalkFiles is a helper to iterates over all known files in the lightroom catalog
func WalkFiles(db *sql.DB, handler func(File, Counter) error, counter Counter) error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	files, DBerrors := ListFiles(ctx, db)
	processErrors := processAllFiles(files, handler, counter)
	select {
	case err := <-DBerrors:
		return err
	case err := <-processErrors:
		return err
	}
}

func processAllFiles(files chan File, handler func(File, Counter) error, counter Counter) chan error {
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

func normExt(ext string) string {
	return strings.ToUpper(ext)
}

func mergeExtensions(originalExt string, pathss ...[]string) []string {
	r := []string{}
	originalExt = normExt(originalExt)
	for _, paths := range pathss {
		for _, p := range paths {
			e := filepath.Ext(p)
			if len(e) > 0 && e[0] == '.' {
				e = e[1:]
			} else {
				e = p
			}
			found := normExt(e) == originalExt
			for _, o := range r {
				if normExt(o) == normExt(e) {
					found = true
				}
			}
			if !found {
				r = append(r, e)
			}
		}
	}
	return r
}

func normalizeExtensions(original []string) []string {
	r := []string{}
	for _, o := range original {
		if o != "" {
			r = append(r, o)
		}
	}
	return r
}

func sameSlices(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i, v := range s1 {
		if s2[i] != v {
			return false
		}
	}
	return true
}

func roundFirst(d time.Duration, candidates ...time.Duration) time.Duration {
	for _, c := range candidates {
		v := d.Round(c)
		if v != 0 {
			return v
		}
	}
	return d
}

func prettyDuration(d time.Duration) string {
	return roundFirst(d, 1*time.Hour, 10*time.Minute, 30*time.Second, 1*time.Second).String()
}
