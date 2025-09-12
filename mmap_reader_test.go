package main

import (
	"fmt"
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
