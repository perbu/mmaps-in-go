package main

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
)

const testFile = "data.bin"

func TestMain(m *testing.M) {
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		createTestFile()
	}
	os.Exit(m.Run())
}

func createTestFile() {
	file, err := os.Create(testFile)
	if err != nil {
		panic(fmt.Sprintf("failed to create test file: %v", err))
	}
	defer file.Close()

	rng := rand.New(rand.NewSource(12345))
	buf := make([]byte, RecordSize)

	for i := 0; i < RecordCount; i++ {
		rng.Read(buf)
		if _, err := file.Write(buf); err != nil {
			panic(fmt.Sprintf("failed to write test data: %v", err))
		}
	}
}

// BenchmarkRandomAccess measures single random record access performance.
// Uses deterministic random seed for reproducible results across runs.
// This simulates database-style point queries where cache locality is minimal.
func BenchmarkRandomAccess(b *testing.B) {
	b.Run("ReaderAt", func(b *testing.B) {
		reader, err := NewReaderAtReader(testFile)
		if err != nil {
			b.Fatalf("failed to create ReaderAt reader: %v", err)
		}
		defer reader.Close()

		rng := rand.New(rand.NewSource(42))
		indices := make([]int, b.N)
		for i := 0; i < b.N; i++ {
			indices[i] = rng.Intn(RecordCount)
		}

		buf := make([]byte, RecordSize)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := reader.ReadRecord(indices[i], buf)
			if err != nil {
				b.Fatalf("failed to read record %d: %v", indices[i], err)
			}
		}
	})

	b.Run("Mmap", func(b *testing.B) {
		reader, err := NewMmapReader(testFile)
		if err != nil {
			b.Fatalf("failed to create mmap reader: %v", err)
		}
		defer reader.Close()

		rng := rand.New(rand.NewSource(42))
		indices := make([]int, b.N)
		for i := 0; i < b.N; i++ {
			indices[i] = rng.Intn(RecordCount)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := reader.ReadRecord(indices[i], nil)
			if err != nil {
				b.Fatalf("failed to read record %d: %v", indices[i], err)
			}
		}
	})
}

// BenchmarkBatchRandomAccess tests batched random reads with varying batch sizes.
// Simulates workloads that read multiple random records in succession,
// helping evaluate how well each approach handles cache pressure and syscall overhead.
func BenchmarkBatchRandomAccess(b *testing.B) {
	batchSizes := []int{10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("ReaderAt_Batch%d", batchSize), func(b *testing.B) {
			reader, err := NewReaderAtReader(testFile)
			if err != nil {
				b.Fatalf("failed to create ReaderAt reader: %v", err)
			}
			defer reader.Close()

			rng := rand.New(rand.NewSource(42))
			buf := make([]byte, RecordSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < batchSize; j++ {
					index := rng.Intn(RecordCount)
					_, err := reader.ReadRecord(index, buf)
					if err != nil {
						b.Fatalf("failed to read record %d: %v", index, err)
					}
				}
			}
		})

		b.Run(fmt.Sprintf("Mmap_Batch%d", batchSize), func(b *testing.B) {
			reader, err := NewMmapReader(testFile)
			if err != nil {
				b.Fatalf("failed to create mmap reader: %v", err)
			}
			defer reader.Close()

			rng := rand.New(rand.NewSource(42))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < batchSize; j++ {
					index := rng.Intn(RecordCount)
					_, err := reader.ReadRecord(index, nil)
					if err != nil {
						b.Fatalf("failed to read record %d: %v", index, err)
					}
				}
			}
		})
	}
}

// BenchmarkSequentialIteration measures per-record sequential read performance.
// Each benchmark iteration reads one record, letting the Go test framework
// determine optimal iteration count for statistical reliability.
func BenchmarkSequentialIteration(b *testing.B) {
	b.Run("ReaderAt", func(b *testing.B) {
		reader, err := NewReaderAtReader(testFile)
		if err != nil {
			b.Fatalf("failed to create ReaderAt reader: %v", err)
		}
		defer reader.Close()

		buf := make([]byte, RecordSize)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			recordIndex := i % RecordCount
			_, err := reader.ReadRecord(recordIndex, buf)
			if err != nil {
				b.Fatalf("failed to read record %d: %v", recordIndex, err)
			}
		}
	})

	b.Run("Mmap", func(b *testing.B) {
		reader, err := NewMmapReader(testFile)
		if err != nil {
			b.Fatalf("failed to create mmap reader: %v", err)
		}
		defer reader.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			recordIndex := i % RecordCount
			_, err := reader.ReadRecord(recordIndex, nil)
			if err != nil {
				b.Fatalf("failed to read record %d: %v", recordIndex, err)
			}
		}
	})
}

// TestReaderConsistency verifies both implementations return identical data.
// Tests key boundary conditions (first/last records) and various positions
// to ensure functional equivalence before performance comparison.
func TestReaderConsistency(t *testing.T) {
	readerAt, err := NewReaderAtReader(testFile)
	if err != nil {
		t.Fatalf("failed to create ReaderAt reader: %v", err)
	}
	defer readerAt.Close()

	mmapReader, err := NewMmapReader(testFile)
	if err != nil {
		t.Fatalf("failed to create mmap reader: %v", err)
	}
	defer mmapReader.Close()

	testIndices := []int{0, 1, 100, 1000, 10000, 100000, 500000, 999999}
	buf := make([]byte, RecordSize)

	for _, index := range testIndices {
		dataRA, err := readerAt.ReadRecord(index, buf)
		if err != nil {
			t.Fatalf("ReaderAt failed to read record %d: %v", index, err)
		}

		dataMmap, err := mmapReader.ReadRecord(index, nil)
		if err != nil {
			t.Fatalf("Mmap failed to read record %d: %v", index, err)
		}

		if len(dataRA) != len(dataMmap) {
			t.Errorf("Length mismatch at record %d: ReaderAt=%d, Mmap=%d", index, len(dataRA), len(dataMmap))
		}

		for i := 0; i < len(dataRA) && i < len(dataMmap); i++ {
			if dataRA[i] != dataMmap[i] {
				t.Errorf("Data mismatch at record %d, byte %d: ReaderAt=%d, Mmap=%d", index, i, dataRA[i], dataMmap[i])
				break
			}
		}
	}
}

// TestWriterConsistency verifies both writers produce identical results.
func TestWriterConsistency(t *testing.T) {
	// Create a temporary test file for write testing
	tempFile := "write_test.bin"
	defer os.Remove(tempFile)

	// Copy original test file to temp file for testing
	srcFile, err := os.Open(testFile)
	if err != nil {
		t.Fatalf("failed to open source file: %v", err)
	}
	defer srcFile.Close()

	dstFile1, err := os.Create(tempFile + ".1")
	if err != nil {
		t.Fatalf("failed to create temp file 1: %v", err)
	}
	_, err = io.Copy(dstFile1, srcFile)
	dstFile1.Close()
	if err != nil {
		t.Fatalf("failed to copy to temp file 1: %v", err)
	}

	srcFile.Seek(0, 0)
	dstFile2, err := os.Create(tempFile + ".2")
	if err != nil {
		t.Fatalf("failed to create temp file 2: %v", err)
	}
	defer os.Remove(tempFile + ".1")
	defer os.Remove(tempFile + ".2")
	_, err = io.Copy(dstFile2, srcFile)
	dstFile2.Close()
	if err != nil {
		t.Fatalf("failed to copy to temp file 2: %v", err)
	}

	writerAt, err := NewWriterAtWriter(tempFile + ".1")
	if err != nil {
		t.Fatalf("failed to create WriterAt writer: %v", err)
	}
	defer writerAt.Close()

	mmapWriter, err := NewMmapWriter(tempFile + ".2")
	if err != nil {
		t.Fatalf("failed to create mmap writer: %v", err)
	}
	defer mmapWriter.Close()

	// Test writing the same data to both files
	testIndices := []int{0, 1, 100, 1000, 10000, 100000, 500000, 999999}
	rng := rand.New(rand.NewSource(98765))

	for _, index := range testIndices {
		data := make([]byte, RecordSize)
		rng.Read(data)

		err := writerAt.WriteRecord(index, data)
		if err != nil {
			t.Fatalf("WriterAt failed to write record %d: %v", index, err)
		}

		err = mmapWriter.WriteRecord(index, data)
		if err != nil {
			t.Fatalf("Mmap failed to write record %d: %v", index, err)
		}
	}

	// Close writers to ensure data is written
	writerAt.Close()
	mmapWriter.Close()

	// Now verify both files have identical content
	readerAt1, err := NewReaderAtReader(tempFile + ".1")
	if err != nil {
		t.Fatalf("failed to create reader for file 1: %v", err)
	}
	defer readerAt1.Close()

	readerAt2, err := NewReaderAtReader(tempFile + ".2")
	if err != nil {
		t.Fatalf("failed to create reader for file 2: %v", err)
	}
	defer readerAt2.Close()

	buf1 := make([]byte, RecordSize)
	buf2 := make([]byte, RecordSize)

	for _, index := range testIndices {
		data1, err := readerAt1.ReadRecord(index, buf1)
		if err != nil {
			t.Fatalf("Failed to read record %d from file 1: %v", index, err)
		}

		data2, err := readerAt2.ReadRecord(index, buf2)
		if err != nil {
			t.Fatalf("Failed to read record %d from file 2: %v", index, err)
		}

		if len(data1) != len(data2) {
			t.Errorf("Length mismatch at record %d: WriterAt=%d, Mmap=%d", index, len(data1), len(data2))
		}

		for i := 0; i < len(data1) && i < len(data2); i++ {
			if data1[i] != data2[i] {
				t.Errorf("Data mismatch at record %d, byte %d: WriterAt=%d, Mmap=%d", index, i, data1[i], data2[i])
				break
			}
		}
	}
}

// BenchmarkRandomWrite measures single random record write performance.
// Uses deterministic random seed for reproducible results across runs.
func BenchmarkRandomWrite(b *testing.B) {
	b.Run("WriterAt", func(b *testing.B) {
		writer, err := NewWriterAtWriter(testFile)
		if err != nil {
			b.Fatalf("failed to create WriterAt writer: %v", err)
		}
		defer writer.Close()

		rng := rand.New(rand.NewSource(42))
		indices := make([]int, b.N)
		data := make([]byte, RecordSize)
		for i := 0; i < b.N; i++ {
			indices[i] = rng.Intn(RecordCount)
			rng.Read(data)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rng.Read(data)
			err := writer.WriteRecord(indices[i], data)
			if err != nil {
				b.Fatalf("failed to write record %d: %v", indices[i], err)
			}
		}
	})

	b.Run("Mmap", func(b *testing.B) {
		writer, err := NewMmapWriter(testFile)
		if err != nil {
			b.Fatalf("failed to create mmap writer: %v", err)
		}
		defer writer.Close()

		rng := rand.New(rand.NewSource(42))
		indices := make([]int, b.N)
		data := make([]byte, RecordSize)
		for i := 0; i < b.N; i++ {
			indices[i] = rng.Intn(RecordCount)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rng.Read(data)
			err := writer.WriteRecord(indices[i], data)
			if err != nil {
				b.Fatalf("failed to write record %d: %v", indices[i], err)
			}
		}
	})
}

// BenchmarkSequentialWrite measures sequential write performance.
func BenchmarkSequentialWrite(b *testing.B) {
	b.Run("WriterAt", func(b *testing.B) {
		writer, err := NewWriterAtWriter(testFile)
		if err != nil {
			b.Fatalf("failed to create WriterAt writer: %v", err)
		}
		defer writer.Close()

		data := make([]byte, RecordSize)
		rng := rand.New(rand.NewSource(12345))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			recordIndex := i % RecordCount
			rng.Read(data)
			err := writer.WriteRecord(recordIndex, data)
			if err != nil {
				b.Fatalf("failed to write record %d: %v", recordIndex, err)
			}
		}
	})

	b.Run("Mmap", func(b *testing.B) {
		writer, err := NewMmapWriter(testFile)
		if err != nil {
			b.Fatalf("failed to create mmap writer: %v", err)
		}
		defer writer.Close()

		data := make([]byte, RecordSize)
		rng := rand.New(rand.NewSource(12345))

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			recordIndex := i % RecordCount
			rng.Read(data)
			err := writer.WriteRecord(recordIndex, data)
			if err != nil {
				b.Fatalf("failed to write record %d: %v", recordIndex, err)
			}
		}
	})
}

// BenchmarkColdVsWarmPages demonstrates the dramatic performance difference between
// cold pages (not in memory, causing page faults) vs warm pages (already in memory).
// This is the key insight: mmap writes have two completely different performance profiles.
func BenchmarkColdVsWarmPages(b *testing.B) {
	// Cold pages: evict pages from memory before each write to force page faults
	b.Run("Mmap_ColdPages", func(b *testing.B) {
		writer, err := NewMmapWriter(testFile)
		if err != nil {
			b.Fatalf("failed to create mmap writer: %v", err)
		}
		defer writer.Close()

		rng := rand.New(rand.NewSource(42))
		indices := make([]int, b.N)
		data := make([]byte, RecordSize)
		for i := 0; i < b.N; i++ {
			indices[i] = rng.Intn(RecordCount)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Force page eviction to simulate cold pages (page fault on write)
			writer.EvictPages()
			rng.Read(data)
			err := writer.WriteRecord(indices[i], data)
			if err != nil {
				b.Fatalf("failed to write record %d: %v", indices[i], err)
			}
		}
	})

	// Warm pages: pre-touch pages to ensure they're in memory
	b.Run("Mmap_WarmPages", func(b *testing.B) {
		writer, err := NewMmapWriter(testFile)
		if err != nil {
			b.Fatalf("failed to create mmap writer: %v", err)
		}
		defer writer.Close()

		// Pre-warm all pages by touching them
		writer.WarmPages()

		rng := rand.New(rand.NewSource(42))
		indices := make([]int, b.N)
		data := make([]byte, RecordSize)
		for i := 0; i < b.N; i++ {
			indices[i] = rng.Intn(RecordCount)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rng.Read(data)
			err := writer.WriteRecord(indices[i], data)
			if err != nil {
				b.Fatalf("failed to write record %d: %v", indices[i], err)
			}
		}
	})

	// WriterAt for comparison (not affected by page cache in same way)
	b.Run("WriterAt_Baseline", func(b *testing.B) {
		writer, err := NewWriterAtWriter(testFile)
		if err != nil {
			b.Fatalf("failed to create WriterAt writer: %v", err)
		}
		defer writer.Close()

		rng := rand.New(rand.NewSource(42))
		indices := make([]int, b.N)
		data := make([]byte, RecordSize)
		for i := 0; i < b.N; i++ {
			indices[i] = rng.Intn(RecordCount)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rng.Read(data)
			err := writer.WriteRecord(indices[i], data)
			if err != nil {
				b.Fatalf("failed to write record %d: %v", indices[i], err)
			}
		}
	})
}
