# Parquet.Query.Extensions.Pooling

`kiloOhm.Parquet.Net.Query.Extensions.Pooling` adds reusable `ParquetReader` pooling on top of `parquet-dotnet-query`.

It currently includes:

- a per-file `ParquetReaderPool` that reuses open readers and their file handles
- `PrewarmAsync(...)` helpers to fill pools before the first query hits
- `BlockFileAsync(...)` helpers for coordinated file replacement
- `WithReaderPool()` query extensions that route `ParquetQuery` execution through the pool

This package is intended to complement `kiloOhm.Parquet.Net.Query` and `kiloOhm.Parquet.Net`.
