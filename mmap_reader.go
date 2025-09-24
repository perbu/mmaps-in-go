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

type Writer interface {
	WriteRecord(index int, data []byte) error
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

type WriterAtWriter struct {
	file *os.File
}

func NewWriterAtWriter(filename string) (*WriterAtWriter, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file for writing: %w", err)
	}

	return &WriterAtWriter{
		file: file,
	}, nil
}

func (w *WriterAtWriter) WriteRecord(index int, data []byte) error {
	if index < 0 || index >= RecordCount {
		return fmt.Errorf("index %d out of range [0, %d)", index, RecordCount)
	}

	if len(data) != RecordSize {
		return fmt.Errorf("data size mismatch: expected %d bytes, got %d", RecordSize, len(data))
	}

	offset := int64(index * RecordSize)
	n, err := w.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write at offset %d: %w", offset, err)
	}
	if n != RecordSize {
		return fmt.Errorf("partial write: expected %d bytes, wrote %d", RecordSize, n)
	}

	return nil
}

func (w *WriterAtWriter) Close() error {
	return w.file.Close()
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

type MmapWriter struct {
	file *os.File
	data []byte
}

func NewMmapWriter(filename string) (*MmapWriter, error) {
	file, err := os.OpenFile(filename, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file for writing: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	data, err := unix.Mmap(int(file.Fd()), 0, int(stat.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file for writing: %w", err)
	}

	return &MmapWriter{
		file: file,
		data: data,
	}, nil
}

func (w *MmapWriter) WriteRecord(index int, data []byte) error {
	if index < 0 || index >= RecordCount {
		return fmt.Errorf("index %d out of range [0, %d)", index, RecordCount)
	}

	if len(data) != RecordSize {
		return fmt.Errorf("data size mismatch: expected %d bytes, got %d", RecordSize, len(data))
	}

	offset := index * RecordSize
	if offset+RecordSize > len(w.data) {
		return fmt.Errorf("record %d would exceed file bounds", index)
	}

	copy(w.data[offset:offset+RecordSize], data)
	return nil
}

func (w *MmapWriter) Close() error {
	var err1, err2 error
	if w.data != nil {
		err1 = unix.Munmap(w.data)
	}
	if w.file != nil {
		err2 = w.file.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func (w *MmapWriter) EvictPages() error {
	if w.data == nil {
		return nil
	}
	return unix.Madvise(w.data, unix.MADV_DONTNEED)
}

func (w *MmapWriter) WarmPages() {
	if w.data == nil {
		return
	}
	pageSize := 4096
	for i := 0; i < len(w.data); i += pageSize {
		_ = w.data[i]
	}
}
