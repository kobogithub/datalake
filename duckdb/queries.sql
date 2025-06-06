-- =====================================================
-- QUERIES DUCKDB PARA CLASE - CONSULTAS PARQUET
-- Ejemplos desde básico hasta avanzado
-- =====================================================

-- ===== CONFIGURACIÓN INICIAL =====
-- Optimizar DuckDB para mejor rendimiento
SET threads TO -1;  -- Usar todos los cores disponibles
SET memory_limit = '4GB';
SET enable_progress_bar = true;

-- ===== 1. CONSULTAS BÁSICAS - LECTURA DE PARQUET =====

-- Leer un solo archivo Parquet
SELECT * FROM read_parquet('data.parquet') LIMIT 5;

-- Leer múltiples archivos con patrón
SELECT * FROM read_parquet('parquet_data/*.parquet') LIMIT 10;

-- Leer archivos particionados
SELECT * FROM read_parquet('parquet_data/empleados/*/*.parquet') LIMIT 10;

-- Contar registros totales
SELECT COUNT(*) as total_registros 
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet');

-- Ver estructura del archivo (esquema)
DESCRIBE SELECT * FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet');

-- ===== 2. FILTROS Y SELECCIONES =====

-- Filtro básico por columna
SELECT nombre, apellido, salario_anual 
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
WHERE salario_anual > 80000;

-- Filtros múltiples
SELECT * 
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
WHERE departamento = 'departamento=it' 
  AND performance_score >= 4.0
  AND años_experiencia > 3;

-- Filtro con LIKE para búsqueda de texto
SELECT nombre, apellido, cargo
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
WHERE cargo LIKE '%Manager%' OR cargo LIKE '%Director%';

-- Filtro por rango de fechas
SELECT * 
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
WHERE fecha_ingreso BETWEEN '2020-01-01' AND '2023-12-31';

-- ===== 3. AGREGACIONES BÁSICAS =====

-- Estadísticas básicas
SELECT 
    COUNT(*) as total_empleados,
    AVG(salario_anual) as salario_promedio,
    MIN(salario_anual) as salario_minimo,
    MAX(salario_anual) as salario_maximo,
    STDDEV(salario_anual) as desviacion_estandar
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet');

-- Agrupar por departamento
SELECT 
    REPLACE(departamento, 'departamento=', '') as dept,
    COUNT(*) as empleados,
    ROUND(AVG(salario_anual), 0) as salario_promedio,
    ROUND(AVG(performance_score), 2) as performance_promedio
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
GROUP BY departamento
ORDER BY salario_promedio DESC;

-- Top 10 salarios más altos
SELECT 
    nombre, 
    apellido,
    REPLACE(departamento, 'departamento=', '') as dept,
    salario_anual,
    performance_score
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
ORDER BY salario_anual DESC 
LIMIT 10;

-- ===== 4. FUNCIONES DE VENTANA (WINDOW FUNCTIONS) =====

-- Ranking de salarios por departamento
SELECT 
    nombre,
    apellido,
    REPLACE(departamento, 'departamento=', '') as dept,
    salario_anual,
    RANK() OVER (PARTITION BY departamento ORDER BY salario_anual DESC) as ranking_dept
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
ORDER BY dept, ranking_dept;

-- Percentiles de salarios
SELECT 
    nombre,
    apellido,
    salario_anual,
    NTILE(4) OVER (ORDER BY salario_anual) as cuartil_salarial,
    PERCENT_RANK() OVER (ORDER BY salario_anual) as percentil
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
ORDER BY salario_anual DESC;

-- Diferencia con promedio del departamento
SELECT 
    nombre,
    apellido,
    REPLACE(departamento, 'departamento=', '') as dept,
    salario_anual,
    ROUND(AVG(salario_anual) OVER (PARTITION BY departamento), 0) as promedio_dept,
    salario_anual - AVG(salario_anual) OVER (PARTITION BY departamento) as diferencia_promedio
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
ORDER BY diferencia_promedio DESC;

-- ===== 5. CONSULTAS CON CASE WHEN =====

-- Categorización de empleados
SELECT 
    nombre,
    apellido,
    salario_anual,
    performance_score,
    CASE 
        WHEN performance_score >= 4.5 THEN 'Excelente'
        WHEN performance_score >= 3.5 THEN 'Bueno'
        WHEN performance_score >= 2.5 THEN 'Regular'
        ELSE 'Necesita mejorar'
    END as categoria_performance,
    CASE 
        WHEN salario_anual >= 100000 THEN 'Alto'
        WHEN salario_anual >= 60000 THEN 'Medio'
        ELSE 'Bajo'
    END as categoria_salario
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
ORDER BY performance_score DESC, salario_anual DESC;

-- Análisis de generaciones
SELECT 
    CASE 
        WHEN edad < 30 THEN 'Gen Z (< 30)'
        WHEN edad < 45 THEN 'Millennials (30-44)'
        WHEN edad < 60 THEN 'Gen X (45-59)'
        ELSE 'Baby Boomers (60+)'
    END as generacion,
    COUNT(*) as empleados,
    ROUND(AVG(salario_anual), 0) as salario_promedio,
    ROUND(AVG(satisfaccion_laboral), 1) as satisfaccion_promedio
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
GROUP BY 1
ORDER BY salario_promedio DESC;

-- ===== 6. CONSULTAS TEMPORALES =====

-- Análisis por año de ingreso
SELECT 
    EXTRACT(YEAR FROM fecha_ingreso) as año_ingreso,
    COUNT(*) as nuevos_empleados,
    ROUND(AVG(salario_anual), 0) as salario_promedio_nuevos,
    ROUND(AVG(performance_score), 2) as performance_promedio
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
WHERE fecha_ingreso IS NOT NULL
GROUP BY EXTRACT(YEAR FROM fecha_ingreso)
ORDER BY año_ingreso DESC;

-- Antigüedad de empleados
SELECT 
    nombre,
    apellido,
    fecha_ingreso,
    DATEDIFF('day', fecha_ingreso, CURRENT_DATE) as dias_en_empresa,
    ROUND(DATEDIFF('day', fecha_ingreso, CURRENT_DATE) / 365.25, 1) as años_en_empresa
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
WHERE fecha_ingreso IS NOT NULL
ORDER BY dias_en_empresa DESC
LIMIT 15;

-- ===== 7. CONSULTAS ESTADÍSTICAS AVANZADAS =====

-- Correlación entre variables
SELECT 
    CORR(salario_anual, performance_score) as corr_salario_performance,
    CORR(años_experiencia, salario_anual) as corr_experiencia_salario,
    CORR(proyectos_completados, performance_score) as corr_proyectos_performance
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet');

-- Distribución salarial con percentiles
SELECT 
    'P10' as percentil, PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY salario_anual) as valor
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
UNION ALL
SELECT 'P25', PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salario_anual)
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
UNION ALL
SELECT 'P50 (Mediana)', PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salario_anual)
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
UNION ALL
SELECT 'P75', PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salario_anual)
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
UNION ALL
SELECT 'P90', PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY salario_anual)
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet');

-- Detección de outliers (valores atípicos)
WITH stats AS (
    SELECT 
        AVG(salario_anual) as promedio,
        STDDEV(salario_anual) as desviacion
    FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
)
SELECT 
    e.nombre,
    e.apellido,
    e.salario_anual,
    ROUND((e.salario_anual - s.promedio) / s.desviacion, 2) as z_score,
    CASE 
        WHEN ABS((e.salario_anual - s.promedio) / s.desviacion) > 2 
        THEN 'Outlier' 
        ELSE 'Normal' 
    END as categoria
FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet') e
CROSS JOIN stats s
WHERE ABS((e.salario_anual - s.promedio) / s.desviacion) > 2
ORDER BY ABS((e.salario_anual - s.promedio) / s.desviacion) DESC;

-- ===== 8. CONSULTAS CON CTE (Common Table Expressions) =====

-- Análisis jerárquico de performance
WITH performance_tiers AS (
    SELECT 
        *,
        CASE 
            WHEN performance_score >= 4.5 THEN 1
            WHEN performance_score >= 3.5 THEN 2
            WHEN performance_score >= 2.5 THEN 3
            ELSE 4
        END as tier
    FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet')
),
tier_stats AS (
    SELECT 
        tier,
        COUNT(*) as empleados,
        AVG(salario_anual) as salario_promedio,
        AVG(satisfaccion_laboral) as satisfaccion_promedio
    FROM performance_tiers
    GROUP BY tier
)
SELECT 
    CASE tier
        WHEN 1 THEN 'Tier 1: Excelente (4.5+)'
        WHEN 2 THEN 'Tier 2: Bueno (3.5-4.5)'
        WHEN 3 THEN 'Tier 3: Regular (2.5-3.5)'
        WHEN 4 THEN 'Tier 4: Bajo (<2.5)'
    END as nivel_performance,
    empleados,
    ROUND(salario_promedio, 0) as salario_promedio,
    ROUND(satisfaccion_promedio, 1) as satisfaccion_promedio,
    ROUND(100.0 * empleados / SUM(empleados) OVER (), 1) as porcentaje_total
FROM tier_stats
ORDER BY tier;

-- ===== 9. CONSULTAS DE MÚLTIPLES ARCHIVOS/DATASETS =====

-- Crear vistas temporales para facilitar consultas
CREATE OR REPLACE VIEW empleados AS 
SELECT * FROM read_parquet('parquet_compressed/empleados_gzip/*/data.parquet');

CREATE OR REPLACE VIEW ventas AS 
SELECT * FROM read_parquet('parquet_compressed/ventas_gzip/*/data.parquet');

-- Consulta cruzada (ejemplo conceptual)
-- Si tuviéramos IDs relacionados entre empleados y ventas:
/*
SELECT 
    e.departamento,
    COUNT(DISTINCT e.empleado_id) as empleados,
    COUNT(v.orden_id) as ventas_realizadas,
    SUM(v.total) as revenue_total
FROM empleados e
LEFT JOIN ventas v ON e.empleado_id = v.vendedor_id
GROUP BY e.departamento
ORDER BY revenue_total DESC;
*/

-- ===== 10. CONSULTAS DE RENDIMIENTO Y OPTIMIZACIÓN =====

-- Explicar plan de ejecución
EXPLAIN SELECT * FROM empleados WHERE salario_anual > 100000;

-- Profiling de consulta
EXPLAIN ANALYZE SELECT 
    departamento,
    COUNT(*) as empleados,
    AVG(salario_anual) as salario_promedio
FROM empleados 
GROUP BY departamento;

-- Información de archivos Parquet
SELECT * FROM parquet_metadata('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- Esquema detallado
SELECT * FROM parquet_schema('parquet_compressed/empleados_gzip/departamento=it/data.parquet');

-- ===== 11. CONSULTAS ÚTILES PARA EXPLORACIÓN DE DATOS =====

-- Resumen rápido del dataset
SELECT 
    'Total empleados' as metrica, COUNT(*)::VARCHAR as valor
FROM empleados
UNION ALL
SELECT 'Departamentos únicos', COUNT(DISTINCT departamento)::VARCHAR
FROM empleados
UNION ALL
SELECT 'Salario promedio', ROUND(AVG(salario_anual), 0)::VARCHAR || ' USD'
FROM empleados
UNION ALL
SELECT 'Performance promedio', ROUND(AVG(performance_score), 2)::VARCHAR
FROM empleados
UNION ALL
SELECT 'Empleado más antiguo', MIN(fecha_ingreso)::VARCHAR
FROM empleados;

-- Valores únicos por columna categórica
SELECT 'Departamentos' as categoria, departamento as valor, COUNT(*) as frecuencia
FROM empleados GROUP BY departamento
UNION ALL
SELECT 'Ubicaciones', ubicacion, COUNT(*)
FROM empleados GROUP BY ubicacion
UNION ALL  
SELECT 'Estados', estado, COUNT(*)
FROM empleados GROUP BY estado
ORDER BY categoria, frecuencia DESC;

-- Datos faltantes (NULL)
SELECT 
    'empleado_id' as columna, 
    COUNT(*) - COUNT(empleado_id) as valores_null,
    ROUND(100.0 * (COUNT(*) - COUNT(empleado_id)) / COUNT(*), 2) as porcentaje_null
FROM empleados
UNION ALL
SELECT 'fecha_ingreso', COUNT(*) - COUNT(fecha_ingreso), 
       ROUND(100.0 * (COUNT(*) - COUNT(fecha_ingreso)) / COUNT(*), 2)
FROM empleados
UNION ALL
SELECT 'performance_score', COUNT(*) - COUNT(performance_score),
       ROUND(100.0 * (COUNT(*) - COUNT(performance_score)) / COUNT(*), 2)
FROM empleados;

-- ===== 12. CONSULTAS PARA PRESENTACIÓN =====

-- Dashboard ejecutivo - Métricas clave
SELECT 
    'KPI' as categoria,
    'Empleados Totales' as metrica,
    COUNT(*)::VARCHAR as valor
FROM empleados
UNION ALL
SELECT 'KPI', 'Costo Salarial Anual', 
       '$' || ROUND(SUM(salario_anual)/1000000, 1)::VARCHAR || 'M'
FROM empleados
UNION ALL
SELECT 'KPI', 'Performance Promedio',
       ROUND(AVG(performance_score), 2)::VARCHAR || '/5.0'
FROM empleados
UNION ALL
SELECT 'KPI', 'Satisfacción Promedio',
       ROUND(AVG(satisfaccion_laboral), 1)::VARCHAR || '/10'
FROM empleados;

-- Reporte por departamento para presentación
SELECT 
    REPLACE(departamento, 'departamento=', '') as "Departamento",
    COUNT(*) as "Empleados",
    '$' || FORMAT('{:,.0f}', ROUND(AVG(salario_anual), 0)) as "Salario Promedio",
    ROUND(AVG(performance_score), 2) as "Performance",
    ROUND(AVG(satisfaccion_laboral), 1) as "Satisfacción",
    ROUND(AVG(años_experiencia), 1) as "Experiencia (años)"
FROM empleados
GROUP BY departamento
ORDER BY AVG(salario_anual) DESC;

-- ===== COMANDOS ÚTILES ADICIONALES =====

-- Listar archivos Parquet disponibles
SELECT * FROM glob('parquet_compressed/**/*.parquet');

-- Información del sistema DuckDB
SELECT * FROM duckdb_settings() WHERE name LIKE '%thread%' OR name LIKE '%memory%';

-- Limpiar caché
CALL duckdb_vacuum();

-- =====================================================
-- FIN DE QUERIES PARA CLASE
-- Copia y pega cualquiera de estas consultas en DuckDB CLI
-- =====================================================