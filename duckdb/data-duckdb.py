import duckdb
import pandas as pd
import os
import json
from pathlib import Path
from datetime import datetime
import glob

class DuckDBParquetAnalyzer:
    """
    Analizador de datos Parquet usando DuckDB para consultas rápidas y eficientes
    """
    
    def __init__(self, parquet_dir="parquet_data", db_file=":memory:"):
        """
        Inicializa el analizador DuckDB
        
        Args:
            parquet_dir: Directorio con archivos Parquet
            db_file: Archivo de base de datos (:memory: para en memoria)
        """
        self.parquet_dir = Path(parquet_dir)
        self.conn = duckdb.connect(db_file)
        
        # Configurar DuckDB para mejor rendimiento
        self.conn.execute("SET threads TO 4")
        self.conn.execute("SET memory_limit = '2GB'")
        
        print(f"✅ DuckDB inicializado")
        print(f"📁 Directorio Parquet: {self.parquet_dir}")
        print(f"💾 Base de datos: {'En memoria' if db_file == ':memory:' else db_file}")

    def detectar_datasets(self):
        """Detecta automáticamente los datasets Parquet disponibles"""
        datasets = {}
        
        if not self.parquet_dir.exists():
            print(f"❌ El directorio {self.parquet_dir} no existe")
            return datasets
        
        for item in self.parquet_dir.iterdir():
            if item.is_dir():
                # Buscar archivos .parquet en el directorio
                parquet_files = list(item.rglob("*.parquet"))
                if parquet_files:
                    datasets[item.name] = {
                        'path': str(item),
                        'files': [str(f) for f in parquet_files],
                        'count': len(parquet_files)
                    }
        
        print(f"🔍 Datasets encontrados: {len(datasets)}")
        for name, info in datasets.items():
            print(f"   📊 {name}: {info['count']} archivo(s)")
        
        return datasets

    def crear_vistas(self, datasets):
        """Crea vistas DuckDB para cada dataset"""
        print(f"\n📋 Creando vistas DuckDB...")
        
        for dataset_name, info in datasets.items():
            try:
                # Crear vista que lea todos los archivos Parquet del dataset
                view_sql = f"""
                CREATE OR REPLACE VIEW {dataset_name} AS 
                SELECT * FROM read_parquet('{info['path']}/**/*.parquet')
                """
                
                self.conn.execute(view_sql)
                
                # Obtener información de la vista
                count_result = self.conn.execute(f"SELECT COUNT(*) FROM {dataset_name}").fetchone()
                count = count_result[0] if count_result else 0
                
                print(f"   ✅ Vista '{dataset_name}': {count:,} registros")
                
            except Exception as e:
                print(f"   ❌ Error creando vista '{dataset_name}': {e}")

    def obtener_esquema(self, tabla):
        """Obtiene el esquema de una tabla/vista"""
        try:
            resultado = self.conn.execute(f"DESCRIBE {tabla}").fetchdf()
            return resultado
        except Exception as e:
            print(f"❌ Error obteniendo esquema de {tabla}: {e}")
            return None

    def ejecutar_consulta(self, sql, descripcion=""):
        """Ejecuta una consulta SQL y devuelve el resultado"""
        try:
            if descripcion:
                print(f"🔍 {descripcion}")
            
            print(f"📝 SQL: {sql}")
            resultado = self.conn.execute(sql).fetchdf()
            print(f"✅ Resultado: {len(resultado)} filas")
            return resultado
            
        except Exception as e:
            print(f"❌ Error en consulta: {e}")
            return None

    def analisis_exploratorio_ventas(self):
        """Análisis exploratorio específico para ventas"""
        print(f"\n🛍️  ANÁLISIS DE VENTAS")
        print("=" * 40)
        
        # 1. Resumen general
        sql_resumen = """
        SELECT 
            COUNT(*) as total_ordenes,
            SUM(total) as revenue_total,
            AVG(total) as ticket_promedio,
            COUNT(DISTINCT cliente_id) as clientes_unicos,
            COUNT(DISTINCT categoria) as categorias
        FROM ventas
        """
        
        resumen = self.ejecutar_consulta(sql_resumen, "Resumen general de ventas")
        if resumen is not None:
            print(resumen.to_string(index=False))
        
        # 2. Ventas por categoría
        print(f"\n📊 Top categorías por revenue:")
        sql_categoria = """
        SELECT 
            categoria,
            COUNT(*) as ordenes,
            SUM(total) as revenue,
            AVG(total) as ticket_promedio
        FROM ventas 
        GROUP BY categoria 
        ORDER BY revenue DESC
        """
        
        cat_result = self.ejecutar_consulta(sql_categoria)
        if cat_result is not None:
            print(cat_result.to_string(index=False))
        
        # 3. Tendencia temporal (si hay columna fecha)
        try:
            print(f"\n📈 Ventas por mes:")
            sql_temporal = """
            SELECT 
                strftime(fecha, '%Y-%m') as mes,
                COUNT(*) as ordenes,
                SUM(total) as revenue
            FROM ventas 
            GROUP BY strftime(fecha, '%Y-%m')
            ORDER BY mes
            """
            
            temporal_result = self.ejecutar_consulta(sql_temporal)
            if temporal_result is not None:
                print(temporal_result.to_string(index=False))
                
        except:
            print("⚠️  No se pudo analizar tendencia temporal")
        
        # 4. Top productos
        print(f"\n🏆 Top 10 productos:")
        sql_productos = """
        SELECT 
            producto,
            COUNT(*) as ventas,
            SUM(total) as revenue
        FROM ventas 
        GROUP BY producto 
        ORDER BY revenue DESC 
        LIMIT 10
        """
        
        productos_result = self.ejecutar_consulta(sql_productos)
        if productos_result is not None:
            print(productos_result.to_string(index=False))

    def analisis_exploratorio_empleados(self):
        """Análisis exploratorio específico para empleados"""
        print(f"\n👥 ANÁLISIS DE EMPLEADOS")
        print("=" * 40)
        
        # 1. Resumen por departamento
        sql_departamentos = """
        SELECT 
            departamento,
            COUNT(*) as empleados,
            AVG(salario_anual) as salario_promedio,
            AVG(performance_score) as performance_promedio,
            AVG(satisfaccion_laboral) as satisfaccion_promedio
        FROM empleados 
        GROUP BY departamento 
        ORDER BY salario_promedio DESC
        """
        
        dept_result = self.ejecutar_consulta(sql_departamentos, "Análisis por departamento")
        if dept_result is not None:
            print(dept_result.round(2).to_string(index=False))
        
        # 2. Correlación salario vs performance
        print(f"\n💰 Correlación salario vs performance:")
        sql_correlacion = """
        SELECT 
            CASE 
                WHEN performance_score >= 4.5 THEN 'Excelente (4.5+)'
                WHEN performance_score >= 3.5 THEN 'Bueno (3.5-4.5)'
                WHEN performance_score >= 2.5 THEN 'Regular (2.5-3.5)'
                ELSE 'Bajo (<2.5)'
            END as performance_nivel,
            COUNT(*) as empleados,
            AVG(salario_anual) as salario_promedio
        FROM empleados 
        GROUP BY 1 
        ORDER BY salario_promedio DESC
        """
        
        corr_result = self.ejecutar_consulta(sql_correlacion)
        if corr_result is not None:
            print(corr_result.to_string(index=False))
        
        # 3. Distribución de edad y experiencia
        print(f"\n📊 Distribución de edad y experiencia:")
        sql_distribucion = """
        SELECT 
            CASE 
                WHEN edad < 30 THEN '< 30 años'
                WHEN edad < 40 THEN '30-39 años'
                WHEN edad < 50 THEN '40-49 años'
                ELSE '50+ años'
            END as rango_edad,
            COUNT(*) as empleados,
            AVG(años_experiencia) as experiencia_promedio,
            AVG(salario_anual) as salario_promedio
        FROM empleados 
        GROUP BY 1 
        ORDER BY experiencia_promedio
        """
        
        dist_result = self.ejecutar_consulta(sql_distribucion)
        if dist_result is not None:
            print(dist_result.round(2).to_string(index=False))

    def analisis_exploratorio_marketing(self):
        """Análisis exploratorio específico para marketing"""
        print(f"\n📈 ANÁLISIS DE MARKETING")
        print("=" * 40)
        
        # 1. Performance por canal
        sql_canales = """
        SELECT 
            canal,
            COUNT(*) as campañas,
            SUM(gasto_real) as inversion_total,
            SUM(conversiones) as conversiones_totales,
            AVG(ctr) as ctr_promedio,
            AVG(roas) as roas_promedio
        FROM marketing 
        GROUP BY canal 
        ORDER BY inversion_total DESC
        """
        
        canales_result = self.ejecutar_consulta(sql_canales, "Performance por canal")
        if canales_result is not None:
            print(canales_result.round(2).to_string(index=False))
        
        # 2. ROI por tipo de campaña
        print(f"\n💰 ROI por tipo de campaña:")
        sql_roi = """
        SELECT 
            tipo_campaña,
            COUNT(*) as campañas,
            AVG(roas) as roas_promedio,
            AVG(ctr) as ctr_promedio,
            SUM(gasto_real) as inversion_total
        FROM marketing 
        GROUP BY tipo_campaña 
        ORDER BY roas_promedio DESC
        """
        
        roi_result = self.ejecutar_consulta(sql_roi)
        if roi_result is not None:
            print(roi_result.round(2).to_string(index=False))
        
        # 3. Eficiencia por audiencia
        print(f"\n🎯 Eficiencia por audiencia objetivo:")
        sql_audiencia = """
        SELECT 
            audiencia_objetivo,
            COUNT(*) as campañas,
            AVG(conversiones) as conversiones_promedio,
            AVG(cpc) as cpc_promedio,
            AVG(roas) as roas_promedio
        FROM marketing 
        GROUP BY audiencia_objetivo 
        ORDER BY roas_promedio DESC
        """
        
        aud_result = self.ejecutar_consulta(sql_audiencia)
        if aud_result is not None:
            print(aud_result.round(2).to_string(index=False))

    def consultas_interactivas(self):
        """Modo interactivo para ejecutar consultas personalizadas"""
        print(f"\n💻 MODO CONSULTAS INTERACTIVAS")
        print("=" * 40)
        print("Escribe consultas SQL o comandos especiales:")
        print("  'help' - Ver comandos disponibles")
        print("  'tables' - Ver tablas disponibles")
        print("  'schema <tabla>' - Ver esquema de una tabla")
        print("  'exit' - Salir del modo interactivo")
        
        while True:
            try:
                query = input("\n🔍 SQL> ").strip()
                
                if query.lower() == 'exit':
                    break
                elif query.lower() == 'help':
                    print("Comandos disponibles:")
                    print("  tables, schema <tabla>, exit")
                    print("  O cualquier consulta SQL válida")
                elif query.lower() == 'tables':
                    tables = self.conn.execute("SHOW TABLES").fetchdf()
                    print("📋 Tablas disponibles:")
                    print(tables.to_string(index=False))
                elif query.lower().startswith('schema '):
                    tabla = query.split(' ', 1)[1]
                    schema = self.obtener_esquema(tabla)
                    if schema is not None:
                        print(f"📊 Esquema de {tabla}:")
                        print(schema.to_string(index=False))
                elif query:
                    resultado = self.ejecutar_consulta(query)
                    if resultado is not None and not resultado.empty:
                        print(resultado.to_string(index=False))
                    elif resultado is not None:
                        print("✅ Consulta ejecutada exitosamente (sin resultados)")
                        
            except KeyboardInterrupt:
                print("\n👋 Saliendo del modo interactivo...")
                break
            except Exception as e:
                print(f"❌ Error: {e}")

    def generar_reporte_completo(self):
        """Genera un reporte completo de análisis"""
        print(f"\n📄 GENERANDO REPORTE COMPLETO...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        reporte_file = f"reporte_analisis_duckdb_{timestamp}.md"
        
        with open(reporte_file, 'w', encoding='utf-8') as f:
            f.write(f"# REPORTE DE ANÁLISIS DE DATOS\n")
            f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Información de tablas
            try:
                tables = self.conn.execute("SHOW TABLES").fetchdf()
                f.write("## 📋 TABLAS DISPONIBLES\n\n")
                for tabla in tables['name']:
                    count = self.conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
                    f.write(f"- **{tabla}**: {count:,} registros\n")
                f.write("\n")
                
                # Esquemas
                f.write("## 📊 ESQUEMAS\n\n")
                for tabla in tables['name']:
                    schema = self.obtener_esquema(tabla)
                    if schema is not None:
                        f.write(f"### {tabla.upper()}\n")
                        f.write(f"```\n{schema.to_string(index=False)}\n```\n\n")
                        
            except Exception as e:
                f.write(f"Error generando información de tablas: {e}\n\n")
            
            f.write("## 🔍 CONSULTAS SUGERIDAS\n\n")
            f.write("### Ventas\n")
            f.write("```sql\n")
            f.write("-- Top 10 productos por revenue\n")
            f.write("SELECT producto, SUM(total) as revenue\n")
            f.write("FROM ventas GROUP BY producto ORDER BY revenue DESC LIMIT 10;\n\n")
            f.write("-- Ventas por mes\n")
            f.write("SELECT strftime(fecha, '%Y-%m') as mes, SUM(total) as revenue\n") 
            f.write("FROM ventas GROUP BY mes ORDER BY mes;\n")
            f.write("```\n\n")
            
            f.write("### Empleados\n")
            f.write("```sql\n")
            f.write("-- Salario promedio por departamento\n")
            f.write("SELECT departamento, AVG(salario_anual) as salario_promedio\n")
            f.write("FROM empleados GROUP BY departamento;\n")
            f.write("```\n\n")
            
            f.write("### Marketing\n")
            f.write("```sql\n")
            f.write("-- ROI por canal\n")
            f.write("SELECT canal, AVG(roas) as roi_promedio\n")
            f.write("FROM marketing GROUP BY canal ORDER BY roi_promedio DESC;\n")
            f.write("```\n\n")
        
        print(f"📄 Reporte guardado: {reporte_file}")

    def ejecutar_analisis_completo(self):
        """Ejecuta análisis completo de todos los datasets"""
        print("🚀 INICIANDO ANÁLISIS COMPLETO CON DUCKDB")
        print("=" * 50)
        
        # Detectar y cargar datasets
        datasets = self.detectar_datasets()
        if not datasets:
            print("❌ No se encontraron datasets Parquet")
            return
        
        # Crear vistas
        self.crear_vistas(datasets)
        
        # Análisis específicos por tipo
        if 'ventas' in datasets:
            self.analisis_exploratorio_ventas()
        
        if 'empleados' in datasets:
            self.analisis_exploratorio_empleados()
        
        if 'marketing' in datasets:
            self.analisis_exploratorio_marketing()
        
        # Generar reporte
        self.generar_reporte_completo()
        
        # Modo interactivo opcional
        print(f"\n🤔 ¿Quieres entrar al modo interactivo? (y/n): ", end="")
        if input().lower().startswith('y'):
            self.consultas_interactivas()

    def __del__(self):
        """Cierra la conexión DuckDB"""
        if hasattr(self, 'conn'):
            self.conn.close()

def main():
    """Función principal"""
    print("🦆 ANALIZADOR PARQUET CON DUCKDB")
    print("=" * 40)
    
    try:
        # Verificar instalación de DuckDB
        import duckdb
        print(f"✅ DuckDB version: {duckdb.__version__}")
        
        # Crear analizador
        analyzer = DuckDBParquetAnalyzer()
        
        # Ejecutar análisis
        analyzer.ejecutar_analisis_completo()
        
        print(f"\n🎉 ¡ANÁLISIS COMPLETADO!")
        
    except ImportError:
        print("❌ DuckDB no está instalado")
        print("💡 Instala con: pip install duckdb")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()