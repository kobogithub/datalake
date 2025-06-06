-- =====================================================
-- METADATA DE ARCHIVOS PARQUET CON DUCKDB
-- Todas las formas de inspeccionar archivos Parquet
-- =====================================================

-- ===== 1. METADATA BÁSICA DEL ARCHIVO =====

-- Ver metadata completa del archivo Parquet
SELECT * FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- Información resumida del archivo
SELECT 
    file_name,
    num_columns,
    num_rows,
    num_row_groups,
    format_version,
    created_by
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- ===== 2. ESQUEMA Y ESTRUCTURA =====

-- Ver esquema detallado del archivo
SELECT * FROM parquet_schema('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- Esquema simplificado - solo nombres y tipos
SELECT 
    name as columna,
    type as tipo_datos,
    type_length,
    repetition_type,
    converted_type
FROM parquet_schema('parquet_compressed/empleados_gzip/departamento=it/data.parquet')
ORDER BY name;

-- Ver esquema con DESCRIBE (más simple)
DESCRIBE SELECT * FROM read_parquet('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- ===== 3. INFORMACIÓN DE ROW GROUPS =====

-- Detalles de todos los row groups
SELECT 
    row_group_id,
    num_rows,
    total_byte_size,
    total_byte_size / 1024.0 / 1024.0 as size_mb
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- Estadísticas por columna en cada row group
SELECT 
    row_group_id,
    column_id,
    file_offset,
    total_compressed_size,
    total_uncompressed_size,
    compression_ratio
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet')
WHERE column_id IS NOT NULL;

-- ===== 4. ESTADÍSTICAS DE COLUMNAS =====

-- Ver estadísticas de columnas (min, max, null_count)
SELECT 
    column_id,
    file_offset,
    num_values,
    null_count,
    distinct_count,
    min_value,
    max_value
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet')
WHERE column_id IS NOT NULL AND min_value IS NOT NULL;

-- ===== 5. INFORMACIÓN DE COMPRESIÓN =====

-- Detalles de compresión por columna
SELECT 
    m.column_id,
    s.name as columna,
    m.total_compressed_size,
    m.total_uncompressed_size,
    ROUND(
        100.0 * (m.total_uncompressed_size - m.total_compressed_size) / m.total_uncompressed_size, 
        2
    ) as compression_ratio_percent,
    m.encodings
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet') m
JOIN parquet_schema('parquet_compressed/empleados_gzip/departamento=it/data.parquet') s 
ON m.column_id = s.schema_element_id
WHERE m.column_id IS NOT NULL;

-- ===== 6. MÚLTIPLES ARCHIVOS - METADATA COMPARATIVA =====

-- Comparar metadata de todos los archivos en un directorio
WITH archivos AS (
    SELECT file FROM glob('parquet_compressed/empleados_gzip/*/data.parquet')
)
SELECT 
    REGEXP_EXTRACT(a.file, 'departamento=([^/]+)', 1) as departamento,
    m.num_rows,
    m.num_columns,
    m.num_row_groups,
    ROUND(SUM(m.total_byte_size) / 1024.0 / 1024.0, 2) as size_mb
FROM archivos a
CROSS JOIN LATERAL parquet_metadata(a.file) m
GROUP BY a.file, m.num_rows, m.num_columns, m.num_row_groups
ORDER BY size_mb DESC;

-- ===== 7. INFORMACIÓN AVANZADA DE ENCODING =====

-- Ver tipos de encoding utilizados
SELECT DISTINCT
    s.name as columna,
    s.type,
    UNNEST(STRING_SPLIT(m.encodings, ',')) as encoding_type
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet') m
JOIN parquet_schema('parquet_compressed/empleados_gzip/departamento=it/data.parquet') s 
ON m.column_id = s.schema_element_id
WHERE m.column_id IS NOT NULL AND m.encodings IS NOT NULL;

-- ===== 8. ANÁLISIS DE CALIDAD DE DATOS DESDE METADATA =====

-- Columnas con muchos valores NULL
SELECT 
    s.name as columna,
    m.num_values,
    m.null_count,
    ROUND(100.0 * m.null_count / m.num_values, 2) as porcentaje_null
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet') m
JOIN parquet_schema('parquet_compressed/empleados_gzip/departamento=it/data.parquet') s 
ON m.column_id = s.schema_element_id
WHERE m.column_id IS NOT NULL
ORDER BY porcentaje_null DESC;

-- Columnas con poca variabilidad (pocos valores distintos)
SELECT 
    s.name as columna,
    m.num_values,
    m.distinct_count,
    ROUND(100.0 * m.distinct_count / m.num_values, 2) as porcentaje_distinct
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet') m
JOIN parquet_schema('parquet_compressed/empleados_gzip/departamento=it/data.parquet') s 
ON m.column_id = s.schema_element_id
WHERE m.column_id IS NOT NULL AND m.distinct_count IS NOT NULL
ORDER BY porcentaje_distinct ASC;

-- ===== 9. METADATA PERSONALIZADA (KEY-VALUE) =====

-- Ver metadata personalizada del archivo (si existe)
SELECT 
    key,
    value
FROM parquet_kv_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- ===== 10. COMPARACIÓN DE ARCHIVOS =====

-- Comparar estructura entre diferentes archivos
WITH metadata_comparison AS (
    SELECT 
        'IT' as dept,
        num_rows, num_columns, num_row_groups,
        total_byte_size / 1024.0 / 1024.0 as size_mb
    FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet')
    
    UNION ALL
    
    SELECT 
        'Marketing' as dept,
        num_rows, num_columns, num_row_groups,
        total_byte_size / 1024.0 / 1024.0 as size_mb
    FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=marketing/data.parquet')
    
    UNION ALL
    
    SELECT 
        'Ventas' as dept,
        num_rows, num_columns, num_row_groups,
        total_byte_size / 1024.0 / 1024.0 as size_mb
    FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=ventas/data.parquet')
)
SELECT 
    dept,
    num_rows,
    ROUND(size_mb, 2) as size_mb,
    ROUND(size_mb / num_rows * 1024 * 1024, 2) as bytes_per_row
FROM metadata_comparison
ORDER BY num_rows DESC;

-- ===== 11. FUNCIONES ÚTILES PARA EXPLORACIÓN =====

-- Listar todos los archivos Parquet y su tamaño
SELECT 
    file,
    ROUND(file_size / 1024.0 / 1024.0, 2) as size_mb
FROM (
    SELECT 
        file,
        (SELECT total_byte_size FROM parquet_metadata(file)) as file_size
    FROM glob('parquet_compressed/**/*.parquet')
)
ORDER BY size_mb DESC;

-- Resumen general de todos los archivos
WITH all_files AS (
    SELECT file FROM glob('parquet_compressed/**/*.parquet')
),
file_stats AS (
    SELECT 
        a.file,
        m.num_rows,
        m.num_columns,
        m.total_byte_size
    FROM all_files a
    CROSS JOIN LATERAL parquet_metadata(a.file) m
)
SELECT 
    COUNT(*) as total_archivos,
    SUM(num_rows) as total_registros,
    ROUND(SUM(total_byte_size) / 1024.0 / 1024.0, 2) as total_size_mb,
    ROUND(AVG(num_rows), 0) as promedio_registros_por_archivo,
    MIN(num_columns) as min_columnas,
    MAX(num_columns) as max_columnas
FROM file_stats;

-- ===== 12. QUERIES PRÁCTICAS PARA CLASE =====

-- Mostrar información completa de un archivo de forma organizada
SELECT 
    'Información General' as seccion,
    'Archivo' as metrica,
    file_name as valor
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet')

UNION ALL

SELECT 
    'Información General',
    'Registros',
    num_rows::VARCHAR
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet')

UNION ALL

SELECT 
    'Información General',
    'Columnas',
    num_columns::VARCHAR
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet')

UNION ALL

SELECT 
    'Información General',
    'Tamaño (MB)',
    ROUND(total_byte_size / 1024.0 / 1024.0, 2)::VARCHAR
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet')

UNION ALL

SELECT 
    'Información General',
    'Row Groups',
    num_row_groups::VARCHAR
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet')

UNION ALL

SELECT 
    'Información General',
    'Creado por',
    created_by
FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- Ejemplo de metadata que impresiona en una presentación
SELECT 
    '📊 Estadísticas del Dataset' as titulo,
    '' as detalle
UNION ALL
SELECT 
    '📁 Archivos Parquet',
    COUNT(*)::VARCHAR || ' archivos'
FROM glob('parquet_compressed/**/*.parquet')
UNION ALL
SELECT 
    '💾 Tamaño Total',
    ROUND(SUM(m.total_byte_size) / 1024.0 / 1024.0, 1)::VARCHAR || ' MB'
FROM glob('parquet_compressed/**/*.parquet') g
CROSS JOIN LATERAL parquet_metadata(g.file) m
UNION ALL
SELECT 
    '📈 Registros Totales',
    FORMAT('{:,}', SUM(m.num_rows))
FROM glob('parquet_compressed/**/*.parquet') g
CROSS JOIN LATERAL parquet_metadata(g.file) m
UNION ALL
SELECT 
    '⚡ Compresión Promedio',
    ROUND(AVG(100.0 * (m.total_uncompressed_size - m.total_compressed_size) / m.total_uncompressed_size), 1)::VARCHAR || '%'
FROM glob('parquet_compressed/**/*.parquet') g
CROSS JOIN LATERAL parquet_metadata(g.file) m
WHERE m.column_id IS NOT NULL;

-- =====================================================
-- COMANDOS DIRECTOS PARA COPIAR Y PEGAR
-- =====================================================

-- Ver metadata básica
-- SELECT * FROM parquet_metadata('tu_archivo.parquet');

-- Ver esquema
-- SELECT * FROM parquet_schema('tu_archivo.parquet');

-- Ver metadata de todos los archivos
-- SELECT file, num_rows, total_byte_size FROM glob('**/*.parquet') g CROSS JOIN LATERAL parquet_metadata(g.file) m;