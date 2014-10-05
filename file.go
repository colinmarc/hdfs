package hdfs

import "os"

const clientName = "go-hdfs"

// HdfsFile represents a file or directory in HDFS. It implements the Writer,
// SectionReader, and Closer interfaces, but not SectionWriter or Seeker.
type HdfsFile struct {
	name string
	closed bool
}

// Open returns an HdfsFile which can be used for reading.
func Open(name string) (file *HdfsFile, err error) {
	return &HdfsFile{name, true}, nil
}

// Create creates the named file, overwriting it if it already exists.
// The file can then be used for reading and writing.
func Create(name string, perm os.FileMode) (file *HdfsFile, err error) {
	return &HdfsFile{name, true}, nil
}

// Name returns the name of the file.
func (f *HdfsFile) Name() string {
	return f.name
}

// Read reads up to len(b) bytes from the HdfsFile. It returns the number of bytes
// read and an error, if any. EOF is signaled by a zero count with err set to
// io.EOF.
func (f *HdfsFile) Read(b []byte) (n int, err error) {
	return 0, nil
}

// ReadAt reads len(b) bytes from the HdfsFile starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always returns
// a non-nil error when n < len(b). At end of file, that error is io.EOF.
func (f *HdfsFile) ReadAt(b []byte, off int64) (n int, err error) {
	return 0, nil
}

// Readdir reads the contents of the directory associated with file and returns
// a slice of up to n os.FileInfo values, as would be returned by Stat, in
// directory order. Subsequent calls on the same file will yield further
// os.FileInfos.
//
// If n > 0, Readdir returns at most n os.FileInfo values. In this case, if
// Readdir returns an empty slice, it will return a non-nil error explaining
// why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the os.FileInfo from the directory in a single
// slice. In this case, if Readdir succeeds (reads all the way to the end of
// the directory), it returns the slice and a nil error. If it encounters an
// error before the end of the directory, Readdir returns the os.FileInfo read
// until that point and a non-nil error.
func (f *HdfsFile) Readdir(n int) (fi []os.FileInfo, err error) {
	return []os.FileInfo{}, nil
}

// Readdirnames reads and returns a slice of names from the directory f.
//
// If n > 0, Readdirnames returns at most n names. In this case, if Readdirnames
// returns an empty slice, it will return a non-nil error explaining why. At the
// end of a directory, the error is io.EOF.
//
// If n <= 0, Readdirnames returns all the names from the directory in a single
// slice. In this case, if Readdirnames succeeds (reads all the way to the end
// of the directory), it returns the slice and a nil error. If it encounters an
// error before the end of the directory, Readdirnames returns the names read
// until that point and a non-nil error.
func (f *HdfsFile) Readdirnames(n int) (names []string, err error) {
	return []string{}, nil
}

// Write writes len(b) bytes to the HdfsFile. It returns the number of bytes written
// and an error, if any. Write returns a non-nil error when n != len(b).
//
// Unlike a regular os.File, the contents will be buffered into memory
func (f *HdfsFile) Write(b []byte) (n int, err error) {
	return 0, nil
}

// WriteAt writes len(b) bytes to the HdfsFile starting at byte offset off.
// It returns the number of bytes written and an error, if any. WriteAt returns
// a non-nil error when n != len(b).
func (f *HdfsFile) WriteAt(b []byte, off int64) (n int, err error) {
	return 0, nil
}

// WriteString is like Write, but writes the contents of string s rather than a
// slice of bytes.
func (f *HdfsFile) WriteString(s string) (ret int, err error) {
	return 0, nil
}

// Chmod changes the mode of the file to mode.
func (f *HdfsFile) Chmod(mode os.FileMode) error {
	return nil
}

// Chown changes the numeric uid and gid of the named file.
func (f *HdfsFile) Chown(uid, gid int) error {
	return nil
}

// Close closes the HdfsFile.
func (f *HdfsFile) Close() error {
	return nil
}

