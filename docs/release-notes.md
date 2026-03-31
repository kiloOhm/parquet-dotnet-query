# 0.1.0-preview.5

### Parquet Viewer (new)

- Add `Parquet.Query.Viewer`, a MAUI + WebView2 desktop app for inspecting Parquet files with a React frontend.
- Predicate-based query execution with row group pruning using column statistics and Lucene footer indexes, including fuzzy matching with configurable Levenshtein/Damerau-Levenshtein distance.
- Indices viewer showing custom footer indexes (hash, bitmap, sort order) and built-in column optimizations (statistics, bloom filters, page indexes, sort order).
- Reusable virtualized data grid with resizable columns and complex nested value inspection.
- Encryption key management with per-file persistence across sessions.
- Query editor with predicate builder, row group plan visualization, and C# code generation from predicates.
- Migrate viewer frontend from Tailwind CSS v3 to v4 and from Radix UI to Base UI.

### Indexing Extensions

- Add footer sort order index type (`FooterSortOrderIndexingStrategy`) that writes column sort order metadata to the parquet footer, enabling downstream readers to detect physical sort order without scanning data.
- Add dedicated attribute constants and shared name/serialization helpers for built-in footer index types (hash, bitmap, sort order).
- Expand footer hash index to support additional column types.
- Fix footer-backed hash, bitmap, and Lucene index builders to reopen parquet files with the original encryption options, so footer-encrypted files can build and query footer metadata indexes again.
- Preserve encrypted and signed footer formats when metadata rewrites update parquet footer, instead of downgrading encrypted footer files during index persistence.

# 0.1.0-preview.4

- Switch release and package-fallback builds to `kiloOhm.Parquet.Net 5.6.0-pre.3--kiloOhm.5` for both `net8.0` and `net48`, so CI and published packages use the fork that now ships the required page index, encryption, and footer APIs.
- Fix the `net48` sparse-page compatibility path to avoid compile-time dependency on `ParquetDataPage` when the fallback package does not expose that type and cleanly fall back to dense reads instead.

# 0.1.0-preview.3

- Add `net48` support across `kiloOhm.Parquet.Net.Query` and all published extension packages while keeping `net8.0` support in place.
- Route local debug builds to the sibling fork for both `net8.0` and `net48`, so query packages exercise the forked page index, footer encryption, and bloom filter APIs during development.
- Add shared compatibility shims plus targeted framework fallbacks for async disposal, `ValueTask`, Brotli-compressed footer metadata, path partition pruning, and older BCL/string syntax on .NET Framework.
- Fix follow-up regressions uncovered during the port, including partial row materialization naming collisions, pooling/task coordination compatibility, search/indexing footer helpers, and duplicate compatibility-type build warnings.

# 0.1.0-preview.2

- Add `kiloOhm.Parquet.Net.Query.Extensions.Pooling` with reusable `ParquetReaderPool` support, prewarming helpers, coordinated file blocking, and `WithReaderPool()` query extensions.
- Add reusable query planning cache support via `IParquetQueryCache` and `LruParquetQueryCache` for repeated parquet query execution.
- Improve pushdown and execution internals with faster primitive conversions, shared execution helpers, and platform-aware file path handling.
- Expand XML documentation and test coverage across helper, caching, and pooling paths.

# 0.1.0-preview.1

- Initial NuGet packaging and publishing pipeline for `kiloOhm.Parquet.Net.Query`.
- Publish `kiloOhm.Parquet.Net.Query.Extensions.Writing`, `kiloOhm.Parquet.Net.Query.Extensions.Indexing`, `kiloOhm.Parquet.Net.Query.Extensions.Search`, and `kiloOhm.Parquet.Net.Query.Extensions.Pooling` alongside the core query package.
- Add `kiloOhm.Parquet.Net.Query.Extensions` as an umbrella package that installs the core query package and all published extensions.
