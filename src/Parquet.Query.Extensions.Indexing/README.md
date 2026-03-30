# Parquet.Query.Extensions.Indexing

`kiloOhm.Parquet.Net.Query.Extensions.Indexing` adds footer-backed equality indexes on top of `parquet-dotnet-query`.

It currently includes:

- hash-bucket footer indexes for supported scalar equality columns
- bitmap footer indexes for low-cardinality equality filters
- query extensions that register footer-aware predicate planners
- write-time helpers for `[ParquetFooterHashIndex]` and `[ParquetFooterBitmapIndex]` columns

This package is intended to complement `kiloOhm.Parquet.Net.Query`, `kiloOhm.Parquet.Net.Query.Extensions.Writing`, and `kiloOhm.Parquet.Net`.
