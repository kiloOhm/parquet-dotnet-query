# Parquet.Query.Extensions.Writing

`kiloOhm.Parquet.Net.Query.Extensions.Writing` adds write-side helpers for Parquet workloads that benefit from query-aware metadata.

It currently includes:

- write-plan generation for POCO models
- sort-key annotations
- bloom-filter annotations
- external-index descriptors, including package-specific attributes like `[ParquetLuceneIndex]`, `[ParquetFooterHashIndex]`, and `[ParquetFooterBitmapIndex]`
- file-writer helpers that carry the plan into Parquet output

This package is intended to complement [`kiloOhm.Parquet.Net.Query`](https://www.nuget.org/packages/kiloOhm.Parquet.Net.Query/) and [`kiloOhm.Parquet.Net`](https://www.nuget.org/packages/kiloOhm.Parquet.Net/).
