# Memory-Mapped vs ReaderAt Performance Comparison

Go package demonstrating performance differences between standard file I/O (`ReaderAt`) and memory-mapped file access
for a 100MB binary file containing 1 million 100-byte records.

## Usage

```bash
go test -bench=. -benchmem
```

## Results

Memory-mapped access is typically 15-25x faster for random access patterns and sequential reads due to reduced syscall
overhead and better OS-level caching.

Full results:
```
goos: darwin
goarch: arm64
pkg: mmaps-in-go
cpu: Apple M4
BenchmarkRandomAccess
BenchmarkRandomAccess/ReaderAt
BenchmarkRandomAccess/ReaderAt-10  	 2848651	       416.4 ns/op
BenchmarkRandomAccess/Mmap
BenchmarkRandomAccess/Mmap-10      	840294702	         3.376 ns/op
BenchmarkBatchRandomAccess
BenchmarkBatchRandomAccess/ReaderAt_Batch10
BenchmarkBatchRandomAccess/ReaderAt_Batch10-10         	  265372	      4559 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch10
BenchmarkBatchRandomAccess/Mmap_Batch10-10             	32011167	        36.55 ns/op
BenchmarkBatchRandomAccess/ReaderAt_Batch100
BenchmarkBatchRandomAccess/ReaderAt_Batch100-10        	   27334	     43760 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch100
BenchmarkBatchRandomAccess/Mmap_Batch100-10            	 3311898	       364.9 ns/op
BenchmarkBatchRandomAccess/ReaderAt_Batch1000
BenchmarkBatchRandomAccess/ReaderAt_Batch1000-10       	    2701	    455134 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch1000
BenchmarkBatchRandomAccess/Mmap_Batch1000-10           	  340267	      3549 ns/op
BenchmarkSequentialIteration
BenchmarkSequentialIteration/ReaderAt
BenchmarkSequentialIteration/ReaderAt-10               	 3603312	       333.3 ns/op
BenchmarkSequentialIteration/Mmap
BenchmarkSequentialIteration/Mmap-10                   	875010253	         1.370 ns/op
```