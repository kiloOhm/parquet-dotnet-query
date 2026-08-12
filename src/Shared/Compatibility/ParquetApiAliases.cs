#if PARQUET_V6
global using QueryParquetSerializerOptions = Parquet.ParquetOptions;
global using QueryParquetRowGroupReader = Parquet.ParquetRowGroupReader;
#else
global using QueryParquetSerializerOptions = Parquet.Serialization.ParquetSerializerOptions;
global using QueryParquetRowGroupReader = Parquet.IParquetRowGroupReader;
#endif
