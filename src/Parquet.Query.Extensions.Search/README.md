# Parquet.Query.Extensions.Search

`kiloOhm.Parquet.Net.Query.Extensions.Search` adds text-search helpers on top of `parquet-dotnet-query`.

It currently includes:

- Lucene-style exact token matching
- fuzzy token matching with configurable edit distance
- query extensions that register search-aware predicate planners
- footer-backed text index helpers for write-time metadata

This package is intended to complement `kiloOhm.Parquet.Net.Query`, `kiloOhm.Parquet.Net.Query.Extensions.Writing`, and `kiloOhm.Parquet.Net`.
