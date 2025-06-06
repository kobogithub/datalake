import pandas as pd
import os
import json
from datetime import datetime
import glob
from pathlib import Path

class RobustCSVToParquetConverter:
    """
    Conversor CSV a Parquet ultrarrrobosto que evita problemas de tipos de datos
    """
    
    def __init__(self, output_dir="parquet_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        print(f"✅ Conversor robusto inicializado")
        print(f"📁 Directorio de salida: {self.output_dir}")

    def limpiar_tipos_para_parquet(self, df):
        """Limpia y convierte tipos de datos para compatibilidad total con Parquet"""
        print(f"🧹 Limpiando tipos de datos...")
        
        for col in df.columns:
            original_dtype = df[col].dtype
            
            # Convertir category a string
            if df[col].dtype.name == 'category':
                df[col] = df[col].astype('string')
                print(f"   📊 {col}: category -> string")
            
            # Convertir int32/int64 problemáticos
            elif 'int' in str(df[col].dtype):
                df[col] = df[col].astype('int64')
                print(f"   🔢 {col}: {original_dtype} -> int64")
            
            # Convertir float32/float64 problemáticos
            elif 'float' in str(df[col].dtype):
                df[col] = df[col].astype('float64')
                print(f"   💰 {col}: {original_dtype} -> float64")
            
            # Asegurar que las fechas sean datetime64[ns]
            elif 'datetime' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col])
                print(f"   📅 {col}: datetime64[ns]")
            
            # Convertir object a string explícitamente
            elif df[col].dtype == 'object':
                try:
                    # Intentar convertir a datetime primero
                    if 'fecha' in col.lower():
                        df[col] = pd.to_datetime(df[col])
                        print(f"   📅 {col}: object -> datetime64[ns]")
                    else:
                        df[col] = df[col].astype('string')
                        print(f"   📝 {col}: object -> string")
                except:
                    df[col] = df[col].astype('string')
                    print(f"   📝 {col}: object -> string (fallback)")
        
        return df

    def crear_particiones_seguras(self, df, dataset_type):
        """Crea particiones de manera segura"""
        print(f"📁 Creando particiones para {dataset_type}...")
        
        particiones = []
        
        if dataset_type == 'ventas':
            # Preparar columnas de partición
            if 'fecha' in df.columns:
                df['fecha'] = pd.to_datetime(df['fecha'])
                df['año'] = df['fecha'].dt.year.astype('int64')
                df['mes'] = df['fecha'].dt.month.astype('int64')
            
            if 'categoria' in df.columns:
                df['categoria_clean'] = df['categoria'].astype('string').str.lower().str.replace(' ', '_')
            
            # Crear particiones manuales
            if 'año' in df.columns and 'categoria_clean' in df.columns:
                for año in sorted(df['año'].unique()):
                    for categoria in sorted(df['categoria_clean'].unique()):
                        df_part = df[(df['año'] == año) & (df['categoria_clean'] == categoria)].copy()
                        if not df_part.empty:
                            particiones.append({
                                'data': df_part,
                                'path': f"año={año}/categoria={categoria}",
                                'name': f"ventas_año{año}_cat{categoria}"
                            })
        
        elif dataset_type == 'empleados':
            if 'departamento' in df.columns:
                df['departamento_clean'] = df['departamento'].astype('string').str.lower().str.replace(' ', '_')
                
                for dept in sorted(df['departamento_clean'].unique()):
                    df_part = df[df['departamento_clean'] == dept].copy()
                    if not df_part.empty:
                        particiones.append({
                            'data': df_part,
                            'path': f"departamento={dept}",
                            'name': f"empleados_dept{dept}"
                        })
        
        elif dataset_type == 'marketing':
            if 'canal' in df.columns:
                df['canal_clean'] = df['canal'].astype('string').str.lower().str.replace(' ', '_')
                
                for canal in sorted(df['canal_clean'].unique()):
                    df_part = df[df['canal_clean'] == canal].copy()
                    if not df_part.empty:
                        particiones.append({
                            'data': df_part,
                            'path': f"canal={canal}",
                            'name': f"marketing_canal{canal}"
                        })
        
        # Si no se pudieron crear particiones específicas, crear una partición única
        if not particiones:
            particiones.append({
                'data': df,
                'path': '',
                'name': dataset_type
            })
        
        print(f"   ✅ {len(particiones)} partición(es) creadas")
        return particiones

    def convertir_csv_robusto(self, csv_file):
        """Convierte CSV a Parquet de manera ultrarrrobusta"""
        print(f"\n🔄 Procesando: {csv_file}")
        
        try:
            # Detectar tipo
            filename = os.path.basename(csv_file).lower()
            if 'venta' in filename or 'ecommerce' in filename:
                dataset_type = 'ventas'
            elif 'empleado' in filename or 'rrhh' in filename:
                dataset_type = 'empleados'
            elif 'marketing' in filename or 'campaña' in filename:
                dataset_type = 'marketing'
            else:
                dataset_type = 'otros'
            
            print(f"📊 Tipo detectado: {dataset_type}")
            
            # Cargar CSV
            print(f"📖 Cargando CSV...")
            df = pd.read_csv(csv_file)
            print(f"   📈 {len(df):,} registros, {len(df.columns)} columnas")
            
            # Limpiar tipos
            df = self.limpiar_tipos_para_parquet(df)
            
            # Crear particiones
            particiones = self.crear_particiones_seguras(df, dataset_type)
            
            # Preparar directorio
            output_dataset_dir = self.output_dir / dataset_type
            output_dataset_dir.mkdir(exist_ok=True)
            
            # Guardar cada partición
            csv_size_mb = os.path.getsize(csv_file) / (1024**2)
            parquet_size_mb = 0
            archivos_generados = []
            
            for i, particion in enumerate(particiones):
                data = particion['data']
                path = particion['path']
                
                # Crear directorio de partición
                if path:
                    full_path = output_dataset_dir / path
                    full_path.mkdir(parents=True, exist_ok=True)
                    parquet_file = full_path / f"data.parquet"
                else:
                    parquet_file = output_dataset_dir / f"{Path(csv_file).stem}.parquet"
                
                # Escribir Parquet con configuración segura
                try:
                    data.to_parquet(
                        parquet_file, 
                        engine='pyarrow',
                        compression='snappy',
                        index=False
                    )
                    
                    file_size = parquet_file.stat().st_size / (1024**2)
                    parquet_size_mb += file_size
                    archivos_generados.append(str(parquet_file))
                    
                    print(f"   ✅ {parquet_file} ({len(data):,} registros, {file_size:.2f}MB)")
                    
                except Exception as e:
                    print(f"   ❌ Error escribiendo {parquet_file}: {e}")
                    # Fallback: escribir sin compresión
                    try:
                        data.to_parquet(parquet_file, engine='pyarrow', index=False)
                        file_size = parquet_file.stat().st_size / (1024**2)
                        parquet_size_mb += file_size
                        archivos_generados.append(str(parquet_file))
                        print(f"   ✅ {parquet_file} (sin compresión)")
                    except Exception as e2:
                        print(f"   ❌ Error fatal: {e2}")
            
            # Crear metadata
            metadata = {
                'dataset_info': {
                    'type': dataset_type,
                    'source_file': csv_file,
                    'created_at': datetime.now().isoformat(),
                    'total_records': len(df),
                    'total_columns': len(df.columns),
                    'partitions': len(particiones),
                    'files_generated': len(archivos_generados)
                },
                'size_info': {
                    'csv_size_mb': round(csv_size_mb, 2),
                    'parquet_size_mb': round(parquet_size_mb, 2),
                    'compression_ratio': round((csv_size_mb - parquet_size_mb) / csv_size_mb * 100, 1) if csv_size_mb > 0 else 0
                },
                'schema': {
                    col: str(df[col].dtype) for col in df.columns
                },
                'files': archivos_generados
            }
            
            # Guardar metadata
            metadata_file = output_dataset_dir / f"{Path(csv_file).stem}_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            # Resumen
            compression_ratio = metadata['size_info']['compression_ratio']
            print(f"📄 Metadata: {metadata_file}")
            print(f"🗜️  Compresión: {csv_size_mb:.1f}MB → {parquet_size_mb:.1f}MB ({compression_ratio}%)")
            
            return metadata
            
        except Exception as e:
            print(f"❌ Error procesando {csv_file}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def convertir_todos_robustamente(self):
        """Convierte todos los CSVs de manera robusta"""
        print("🚀 CONVERSIÓN ROBUSTA CSV → PARQUET")
        print("=" * 45)
        
        csv_files = glob.glob("*.csv")
        if not csv_files:
            print("❌ No se encontraron archivos CSV")
            return []
        
        print(f"📁 Encontrados {len(csv_files)} archivos CSV:")
        for csv_file in csv_files:
            print(f"   📄 {csv_file}")
        
        resultados = []
        for csv_file in csv_files:
            resultado = self.convertir_csv_robusto(csv_file)
            if resultado:
                resultados.append(resultado)
        
        # Reporte final
        if resultados:
            self.crear_reporte_final(resultados)
        
        return resultados

    def crear_reporte_final(self, resultados):
        """Crea reporte final de la conversión"""
        print(f"\n📊 RESUMEN FINAL:")
        print("=" * 30)
        
        total_csv = sum(r['size_info']['csv_size_mb'] for r in resultados)
        total_parquet = sum(r['size_info']['parquet_size_mb'] for r in resultados)
        total_compression = (total_csv - total_parquet) / total_csv * 100 if total_csv > 0 else 0
        total_files = sum(r['dataset_info']['files_generated'] for r in resultados)
        
        print(f"✅ Archivos convertidos: {len(resultados)}")
        print(f"📦 Tamaño original: {total_csv:.1f} MB")
        print(f"🗜️  Tamaño final: {total_parquet:.1f} MB")
        print(f"💾 Compresión total: {total_compression:.1f}%")
        print(f"📁 Archivos Parquet: {total_files}")
        print(f"💰 Espacio ahorrado: {total_csv - total_parquet:.1f} MB")
        
        print(f"\n📂 ESTRUCTURA:")
        for root, dirs, files in os.walk(self.output_dir):
            level = root.replace(str(self.output_dir), '').count(os.sep)
            indent = '  ' * level
            print(f"{indent}{os.path.basename(root)}/")
            
        print(f"\n🔍 PARA USAR CON DUCKDB:")
        print("import duckdb")
        print("conn = duckdb.connect()")
        for resultado in resultados:
            dataset_type = resultado['dataset_info']['type']
            print(f"# {dataset_type}")
            print(f"df_{dataset_type} = conn.execute(\"SELECT * FROM read_parquet('parquet_data/{dataset_type}/**/*.parquet')\").df()")

def main():
    """Función principal robusta"""
    print("🛡️  CONVERSOR CSV → PARQUET ULTRAROBUSTO")
    print("=" * 50)
    
    try:
        converter = RobustCSVToParquetConverter()
        resultados = converter.convertir_todos_robustamente()
        
        if resultados:
            print(f"\n🎉 ¡CONVERSIÓN EXITOSA!")
            print("🚀 Los archivos Parquet están listos para DuckDB")
        else:
            print("❌ No se pudieron convertir archivos")
            
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()