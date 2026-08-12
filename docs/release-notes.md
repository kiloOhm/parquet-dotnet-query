# 0.2.1-preview.6.3

### Core Query

- Decode string columns by reusing one `string` instance per distinct value in a row group instead of allocating one per row. parquet-dotnet v6 stores string data as `ReadOnlyMemory<char>` and its `ReadAsync(DataField, Memory<string?>, ...)` convenience overload materializes it by allocating a fresh string for every row, which made reading a filter column scale with row count rather than with distinct value count. Reading a 120 000-row string column measured 20-24 ms on that overload against 10-11 ms on the v5 reader; it now measures 14-16 ms, recovering a little over half of the difference. Columns of unique values allocate exactly what they did before.
- Skip the reuse lookup entirely when a value repeats the previous one, so sort-key columns, which arrive as long runs, cost a single span comparison per row.

### Validation

- Pass all 135 net8 tests and all 133 net48 tests, including new coverage for interleaved nulls, empty strings, non-ASCII values, multiple row groups, values that differ only beyond the reuse hash sample, and filtering on a repeating string column.
- Build the complete solution with zero warnings and errors.

# 0.2.1-preview.6.2

### Encryption

- Derive arbitrary non-empty strings passed to `WithFooterKey(...)` into 256-bit footer keys with SHA-256 before configuring the reader.
- Preserve existing AES key material supplied as 16-, 24-, or 32-byte UTF-8, hexadecimal, Base64, or Base64url strings without deriving it again.

### Validation

- Pass all 131 net8 tests and all 130 net48 tests, including an encrypted-footer round trip using an independently derived string key.
- Build the complete solution, benchmarks, and viewer with zero warnings and errors.

# 0.1.0-preview.7

### Parquet Compatibility

- Upgrade net8 builds to `kiloOhm.Parquet.Net 6.1.1-pre.4` while retaining the legacy net48 dependency, with compatibility adapters for the v6 reader, serializer, schema, encryption, and page-index APIs.
- Use parquet-dotnet's authenticated footer metadata update API so bitmap, sort-order, and Lucene footer indexes preserve encrypted and signed plaintext footers.
- Support path-aware encryption key retrieval and page-index fallback scanning introduced across the parquet-dotnet pre.1 through pre.4 releases.

### Core Query

- Preserve predicate pushdown, projection, partial-row materialization, query caching, and dynamic queries across both parquet-dotnet API generations.
- Keep encrypted reader pooling, AAD configuration, column-key resolution, and option fingerprinting compatible with the v6 encryption model.

### Validation

- Pass all 123 tests on both net8 and net48, including encrypted bitmap/Lucene footer indexes and signed plaintext footer preservation.
- Build the complete solution, benchmarks, and viewer with zero warnings and errors.

# 0.1.0-preview.6.2

### Indexing Extensions

- Fix `Where(row => row.EnumCol == EnumValue)` returning zero rows on parquet files with a footer bitmap index over an enum column. Parquet stores enums as their underlying primitive, so `FooterBitmapIndexingStrategy.BuildIndexAsync` formatted bitmap keys from the raw `int` values (e.g. `"1"`), while `FooterIndexPredicatePlanner.TryEvaluateRowGroup` formatted the predicate value via `Enum.ToString()` (e.g. `"EMEA"`). The lookup missed, the planner reported `mayMatch: false`, and every row group was pruned. `FooterIndexValueFormatter.TryFormat` now formats enum values through their underlying primitive (`Convert.ChangeType` to `Enum.GetUnderlyingType`), so build-time and query-time keys line up against existing parquet files — no re-indexing needed.

### Parquet Viewer

- Add Export CSV button to the query result view. Streams all matched rows through the existing `executeQuery` bridge in 5000-row pages, assembles an RFC 4180 CSV (with UTF-8 BOM for Excel), and triggers a browser download. The button lives in the Results tab header and reflects current state via tooltip and loading spinner.

# 0.1.0-preview.6.1

### Core Query

- Fix `Where(row => row.IntCol == (SomeEnum)x)` (and other narrowing/widening casts on closed-over values) silently dropping all rows when the column has a bloom filter. `PredicatePushdownExtractor` strips the `Convert` node when reading the closed value, so the predicate stored an `enum` against an `int` column; the bloom-filter lookup then hashed the wrong CLR type and ruled the row group out. `PushdownPredicateFactory.CreateComparison` now coerces the constant to the column's CLR type (via `ConvertValue` against `selector.Body` after `StripConvert`) before the predicate is built, so bloom, statistics, and row-level evaluation all see the column-typed value.

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
