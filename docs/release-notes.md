# 0.1.0-preview.2

- Add `kiloOhm.Parquet.Net.Query.Extensions.Pooling` with reusable `ParquetReaderPool` support, prewarming helpers, coordinated file blocking, and `WithReaderPool()` query extensions.
- Add reusable query planning cache support via `IParquetQueryCache` and `LruParquetQueryCache` for repeated parquet query execution.
- Improve pushdown and execution internals with faster primitive conversions, shared execution helpers, and platform-aware file path handling.
- Expand XML documentation and test coverage across helper, caching, and pooling paths.

# 0.1.0-preview.1

- Initial NuGet packaging and publishing pipeline for `kiloOhm.Parquet.Net.Query`.
- Publish `kiloOhm.Parquet.Net.Query.Extensions.Writing`, `kiloOhm.Parquet.Net.Query.Extensions.Indexing`, `kiloOhm.Parquet.Net.Query.Extensions.Search`, and `kiloOhm.Parquet.Net.Query.Extensions.Pooling` alongside the core query package.
- Add `kiloOhm.Parquet.Net.Query.Extensions` as an umbrella package that installs the core query package and all published extensions.
