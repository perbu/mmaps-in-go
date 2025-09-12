package main

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

const (
	RecordSize  = 100
	RecordCount = 1000000
)

type Reader interface {
	ReadRecord(index int, buf []byte) ([]byte, error)
	Close() error
}

type ReaderAtReader struct {
	file *os.File
}

func NewReaderAtReader(filename string) (*ReaderAtReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &ReaderAtReader{
		file: file,
	}, nil
}

func (r *ReaderAtReader) ReadRecord(index int, buf []byte) ([]byte, error) {
	if index < 0 || index >= RecordCount {
		return nil, fmt.Errorf("index %d out of range [0, %d)", index, RecordCount)
	}

	if len(buf) < RecordSize {
		return nil, fmt.Errorf("buffer too small: need %d bytes, got %d", RecordSize, len(buf))
	}

	offset := int64(index * RecordSize)
	n, err := r.file.ReadAt(buf[:RecordSize], offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read at offset %d: %w", offset, err)
	}
	if n != RecordSize {
		return nil, fmt.Errorf("partial read: expected %d bytes, got %d", RecordSize, n)
	}

	return buf[:RecordSize], nil
}

func (r *ReaderAtReader) Close() error {
	return r.file.Close()
}

type MmapReader struct {
	file *os.File
	data []byte
}

func NewMmapReader(filename string) (*MmapReader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	data, err := unix.Mmap(int(file.Fd()), 0, int(stat.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	return &MmapReader{
		file: file,
		data: data,
	}, nil
}

func (m *MmapReader) ReadRecord(index int, buf []byte) ([]byte, error) {
	if index < 0 || index >= RecordCount {
		return nil, fmt.Errorf("index %d out of range [0, %d)", index, RecordCount)
	}

	offset := index * RecordSize
	if offset+RecordSize > len(m.data) {
		return nil, fmt.Errorf("record %d would exceed file bounds", index)
	}

	return m.data[offset : offset+RecordSize], nil
}

func (m *MmapReader) Close() error {
	var err1, err2 error
	if m.data != nil {
		err1 = unix.Munmap(m.data)
	}
	if m.file != nil {
		err2 = m.file.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}
