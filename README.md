# Memory-Mapped vs ReaderAt Performance Comparison

Go package demonstrating performance differences between standard file I/O (`ReaderAt`) and memory-mapped file access
for a 100MB binary file containing 1 million 100-byte records.

## Usage

```bash
go test -bench=. -benchmem
```

## Results, Linux


```
goos: linux
goarch: amd64
pkg: mmaps-in-go
cpu: AMD Ryzen 7 9800X3D 8-Core Processor
BenchmarkRandomAccess/ReaderAt-16                        6017436             186.0 ns/op
BenchmarkRandomAccess/Mmap-16                          958784758             1.253 ns/op
BenchmarkBatchRandomAccess/ReaderAt_Batch10-16            640561              1956 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch10-16              29301102             42.44 ns/op
BenchmarkBatchRandomAccess/ReaderAt_Batch100-16            64110             17896 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch100-16              2920698             417.7 ns/op
BenchmarkBatchRandomAccess/ReaderAt_Batch1000-16            6356            180232 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch1000-16              291384              4076 ns/op
BenchmarkSequentialIteration/ReaderAt-16                 7834741             151.4 ns/op
BenchmarkSequentialIteration/Mmap-16                   877167153             1.368 ns/op
```

## Results, MacOS

```
goos: darwin
goarch: arm64
pkg: mmaps-in-go
cpu: Apple M4
BenchmarkRandomAccess/ReaderAt-10                       2495859              483.5 ns/op
BenchmarkRandomAccess/Mmap-10                          83527287              2.699 ns/op
BenchmarkBatchRandomAccess/ReaderAt_Batch10-10           242306               5425 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch10-10             31778749              36.83 ns/op
BenchmarkBatchRandomAccess/ReaderAt_Batch100-10           24813              49514 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch100-10             3311750              361.8 ns/op
BenchmarkBatchRandomAccess/ReaderAt_Batch1000-10           2502             471698 ns/op
BenchmarkBatchRandomAccess/Mmap_Batch1000-10             332607               3568 ns/op
BenchmarkSequentialIteration/ReaderAt-10                3491724              343.3 ns/op
BenchmarkSequentialIteration/Mmap-10                    8749039              1.376 ns/op
BenchmarkRandomWrite/WriterAt-10                         937342               1260 ns/op
BenchmarkRandomWrite/Mmap-10                           13493138              89.58 ns/op
BenchmarkSequentialWrite/WriterAt-10                    1000000               1126 ns/op
BenchmarkSequentialWrite/Mmap-10                       12440222              81.60 ns/op
BenchmarkColdVsWarmPages/Mmap_ColdPages-10                 8928             298419 ns/op
BenchmarkColdVsWarmPages/Mmap_WarmPages-10             13885890              85.08 ns/op
BenchmarkColdVsWarmPages/WriterAt_Baseline-10            933878               1252 ns/op
```