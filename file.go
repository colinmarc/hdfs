package hdfs

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"io"
	"os"
)

const clientName = "go-hdfs"

// A File represents a file or directory in HDFS. It implements Reader, Writer,
// ReaderAt, WriterAt, Seeker, and Closer.
type File struct {
	client *Client
	name   string
	info   os.FileInfo

	blocks             []*hdfs.LocatedBlockProto
	currentBlockReader *rpc.BlockReader
	offset             int64

	closed       bool
	allowWriting bool
}

// Open returns an File which can be used for reading.
func (c *Client) Open(name string) (file *File, err error) {
	info, err := c.Stat(name)
	if err != nil {
		return nil, err
	}

	return &File{
		client:       c,
		name:         name,
		info:         info,
		closed:       false,
		allowWriting: false,
	}, nil
}

// Create creates the named file, overwriting it if it already exists.
// The file can then be used for reading and writing.
func (c *Client) Create(name string, perm os.FileMode) (file *File, err error) {
	return &File{
		client:       c,
		name:         name,
		closed:       true,
		allowWriting: true,
	}, nil
}

// Name returns the name of the file.
func (f *File) Name() string {
	return f.name
}

// Seek sets the offset for the next Read or Write to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end. Seek returns
// the new offset and an error, if any.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	var off int64
	if whence == 0 {
		off = offset
	} else if whence == 1 {
		off = f.offset + offset
	} else if whence == 2 {
		off = f.info.Size() - offset
	} else {
		return f.offset, fmt.Errorf("Invalid whence: %d", whence)
	}

	if off < 0 || off > f.info.Size() {
		return f.offset, fmt.Errorf("Invalid resulting offset: %d", off)
	}

	f.offset = off
	f.currentBlockReader = nil

	return f.offset, nil
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes
// read and an error, if any. EOF is signaled by a zero count with err set to
// io.EOF.
func (f *File) Read(b []byte) (n int, err error) {
	if f.offset >= f.info.Size() {
		return 0, io.EOF
	}

	if f.blocks == nil {
		err := f.getBlocks()
		if err != nil {
			return 0, err
		}
	}

	if f.currentBlockReader == nil {
		err = f.getNewBlockReader()
		if err != nil {
			return 0, err
		}
	}

	for {
		n, err := f.currentBlockReader.Read(b)
		f.offset += int64(n)

		if err != nil && err != io.EOF {
			f.currentBlockReader.Close()
			f.currentBlockReader = nil
			return n, err
		} else if n > 0 {
			return n, nil
		} else {
			f.currentBlockReader.Close()
			f.getNewBlockReader()
		}
	}
}

// ReadAt reads len(b) bytes from the File starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always returns
// a non-nil error when n < len(b). At end of file, that error is io.EOF.
func (f *File) ReadAt(b []byte, off int64) (int, error) {
	_, err := f.Seek(off, 0)
	if err != nil {
		return 0, err
	}

	return f.Read(b)
}

// Write writes len(b) bytes to the File. It returns the number of bytes written
// and an error, if any. Write returns a non-nil error when n != len(b).
//
// Unlike a regular os.File, the contents will be buffered into memory
func (f *File) Write(b []byte) (n int, err error) {
	return 0, nil
}

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// It returns the number of bytes written and an error, if any. WriteAt returns
// a non-nil error when n != len(b).
func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	return 0, nil
}

// Close closes the File.
func (f *File) Close() error {
	return nil
}

// Chmod changes the mode of the file to mode.
func (f *File) Chmod(mode os.FileMode) error {
	return nil
}

// Chown changes the numeric uid and gid of the named file.
func (f *File) Chown(uid, gid int) error {
	return nil
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
func (f *File) Readdir(n int) (fi []os.FileInfo, err error) {
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
func (f *File) Readdirnames(n int) (names []string, err error) {
	return []string{}, nil
}

func (f *File) getBlocks() error {
	req := &hdfs.GetBlockLocationsRequestProto{
		Src:    proto.String(f.name),
		Offset: proto.Uint64(0),
		Length: proto.Uint64(uint64(f.info.Size())),
	}
	resp := &hdfs.GetBlockLocationsResponseProto{}

	err := f.client.namenode.Execute("getBlockLocations", req, resp)
	if err != nil {
		return err
	}

	f.blocks = resp.GetLocations().GetBlocks()
	return nil
}

func (f *File) getNewBlockReader() error {
	off := uint64(f.offset)
	for _, block := range f.blocks {
		start := block.GetOffset()
		end := start + block.GetB().GetNumBytes()

		if start <= off && off < end {
			br, err := rpc.NewBlockReader(block, off-start)
			if err != nil {
				return err
			}

			f.currentBlockReader = br
			return nil
		}
	}

	return fmt.Errorf("Couldn't find block for offset: %d", off)
}
