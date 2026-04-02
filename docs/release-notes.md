# 0.1.0-preview.6

### Core Query

- Add `DynamicParquetQuery` API for schema-agnostic query execution over already-open `ParquetReader` instances. Supports the full pushdown planning pipeline — including footer index planners for Lucene, bitmap, and bloom filter pruning — without requiring a compile-time row type.

### Writing Extensions

- Auto-configure bloom filter metadata for columns annotated with `BloomFilter` index descriptors. `ParquetWritePlan.CreateSerializerOptions` now populates `BloomFilterOptionsByColumn` automatically so writers no longer need to set bloom filter options manually.
- Deep-clone `ParquetOptions` in `SerializerOptionsSnapshot` to prevent mutation of shared state when bloom filter columns are injected.

### Indexing Extensions

- Remove hash footer indexes (`FooterHashIndexingStrategy`, `[ParquetFooterHashIndex]`) in favor of built-in Parquet bloom filters, which provide the same equality-pruning capability with better space efficiency and no custom metadata overhead.
- Update diagnostics to suggest bloom filters instead of hash indexes for high-cardinality columns.

### Parquet Viewer

- Wire query execution and plan generation to the library's `DynamicParquetQuery` pipeline, replacing hand-rolled `PredicateEvaluator`. Footer index planners (Lucene, bitmap) now participate in row group pruning from the viewer.
- Add browsable index data to the Indices tab: bitmap indexes show all distinct values with row group presence, Lucene indexes show all indexed terms with inverted row group mappings. Entries are filterable and scrollable with colored row group dots.
- Replace classic pagination with scrubber-style virtual scroll: the scrollbar reflects the full row count from the start, and data chunks are fetched on demand as the user scrolls (debounced 150 ms). A sparse LRU cache holds up to ~100 K rows in memory with shimmer placeholders for chunks still loading.
- Add "Go to row" input in the status bar to jump directly to any row by number.
- Fix concurrent read crashes on encrypted Parquet files by serializing reader access with a `SemaphoreSlim`. Multiple parallel chunk requests no longer corrupt the shared file stream position.
- Fix table overflow / missing scrollbar caused by `TabsContent` not being a flex column container.
- Support Windows "Open with" shell association: the app accepts a file path as a command-line argument and opens it automatically on launch.
- Add crash logging to `ParquetViewer.log` next to the executable for diagnosing startup and runtime failures.
- Rename output executable from `Parquet.Query.Viewer.exe` to `ParquetViewer.exe`.

### CI

- Add MAUI viewer build job (Windows x64) to GitHub Actions with React pre-build and artifact upload.
- Publish both framework-dependent and self-contained portable viewer zips attached to GitHub releases.
- Create GitHub releases for preview versions (marked as prerelease), not only stable releases.

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
