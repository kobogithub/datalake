# 🗃️ Data Lake & Synthetic Data Pipeline

Pipeline completo para generar datos sintéticos, convertir a formato Parquet optimizado y realizar análisis con DuckDB.

## 🚀 Características

- **Generación de datos sintéticos** con múltiples métodos
- **Conversión CSV → Parquet** con particionamiento inteligente
- **Análisis de datos** con DuckDB optimizado
- **Metadata avanzada** y consultas de rendimiento

## 📁 Estructura del Proyecto

```
.
├── data-synthetic-json/         # Generación con OpenAI/Faker
├── data-synthetic-producer/     # Generador masivo de datos
├── parquet/                     # Conversores CSV → Parquet
├── duckdb/                      # Análisis y consultas
└── requirements.txt             # Dependencias
```

## ⚡ Quick Start

### 1. Instalación
```bash
git clone <repo>
cd datalake
pip install -r requirements.txt
```

### 2. Generar Datos Sintéticos
```bash
# Opción A: Con Faker (recomendado)
cd data-synthetic-json
python data-synthetic.py

# Opción B: Generador masivo
cd data-synthetic-producer  
python data-synthetic-producer.py
```

### 3. Convertir a Parquet
```bash
cd parquet
# Conversión básica
python data-parquet.py

# Con compresión específica (gzip, snappy, brotli)
python data-parquet-comprimido.py
```

### 4. Análisis con DuckDB
```bash
cd duckdb
# Análisis interactivo
python duckdb.py

# O directamente en DuckDB CLI
duckdb
D SELECT * FROM read_parquet('../parquet_data/**/*.parquet') LIMIT 5;
```

## 📊 Datasets Generados

| Dataset | Registros | Columnas | Casos de Uso |
|---------|-----------|----------|--------------|
| **Empleados** | 200K+ | 19 | RRHH, análisis salarial, performance |
| **Ventas** | 500K+ | 16 | E-commerce, tendencias, segmentación |
| **Marketing** | 300K+ | 20 | ROI, canales, optimización campañas |

## 🛠️ Componentes

### 🎲 Generación de Datos
- **`data-synthetic.py`**: Faker + distribuciones estadísticas
- **`data-synthetic-producer.py`**: Generación masiva optimizada
- Datos realistas con correlaciones lógicas

### 📦 Conversión Parquet
- **`data-parquet.py`**: Conversor básico con particionamiento
- **`data-parquet-comprimido.py`**: Múltiples opciones de compresión
- Optimización automática de tipos de datos

### 🦆 Análisis DuckDB
- **`duckdb.py`**: Analizador interactivo con consultas predefinidas
- **`queries.sql`**: +50 consultas de ejemplo
- **`metadata.sql`**: Inspección de metadata de archivos Parquet

## 📈 Ejemplos de Uso

### Generar 1M de registros
```python
from data_synthetic_producer import DatasetGeneratorFaker
gen = DatasetGeneratorFaker()
gen.generar_todos_los_datasets()  # CSV files
```

### Convertir a Parquet optimizado
```python
from data_parquet_comprimido import ParquetCompressionConverter
converter = ParquetCompressionConverter(compression='gzip')
converter.convertir_todos_con_compression()
```

### Análisis ultrarrápido
```sql
-- 1M+ registros en <100ms
SELECT 
    departamento,
    COUNT(*) as empleados,
    AVG(salario_anual) as salario_promedio
FROM read_parquet('parquet_data/empleados_gzip/**/*.parquet')
GROUP BY departamento;
```

## 🔧 Configuración Avanzada

### Variables de Entorno
```bash
export OPENAI_API_KEY="sk-..."  # Para generación con IA
```

### Optimización DuckDB
```sql
SET threads TO -1;              -- Usar todos los cores
SET memory_limit = '4GB';       -- Aumentar memoria
```

### Opciones de Compresión
| Compresión | Velocidad | Tamaño | Uso |
|------------|-----------|---------|-----|
| `snappy` | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Análisis interactivo |
| `gzip` | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Archivado |
| `brotli` | ⭐⭐ | ⭐⭐⭐⭐⭐ | Máxima compresión |

## 📊 Benchmarks

### Rendimiento Típico
- **Generación**: 100K registros/segundo
- **Conversión**: 60-80% reducción de tamaño
- **Consultas**: 1M+ registros en <100ms

### Comparación de Formatos
| Formato | Tamaño | Velocidad Lectura | Compatibilidad |
|---------|--------|------------------|----------------|
| CSV | 100MB | 1x | Universal |
| Parquet | 30MB | 10x | Moderna |

## 🤝 Contribuir

1. Fork el proyecto
2. Crea una rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -am 'Agregar funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

## 📝 Dependencias

```txt
pandas>=2.0.0
pyarrow>=10.0.0
duckdb>=0.9.0
faker>=20.0.0
numpy>=1.24.0
openai>=1.0.0
python-dotenv>=1.0.0
```

## 🔗 Enlaces Útiles

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Parquet Format](https://parquet.apache.org/)
- [Faker Documentation](https://faker.readthedocs.io/)

## 📄 Licencia

MIT License - ver [LICENSE](LICENSE) para detalles.

---

⭐ Si este proyecto te ayuda, ¡dale una estrella!