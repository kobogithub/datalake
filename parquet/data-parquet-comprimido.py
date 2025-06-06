import pandas as pd
import os
import json
import time
from datetime import datetime
import glob
from pathlib import Path

class ParquetCompressionConverter:
    """
    Conversor CSV a Parquet con múltiples opciones de compresión
    Soporta: snappy, gzip, brotli, lz4, zstd, none
    """
    
    def __init__(self, output_dir="parquet_compressed", compression="snappy"):
        """
        Inicializa el conversor con compresión específica
        
        Args:
            output_dir: Directorio de salida
            compression: Tipo de compresión ('snappy', 'gzip', 'brotli', 'lz4', 'zstd', 'none')
        """
        self.output_dir = Path(output_dir)
        self.compression = compression
        self.output_dir.mkdir(exist_ok=True)
        
        # Información sobre tipos de compresión
        self.compression_info = {
            'snappy': {
                'description': 'Rápido, compresión moderada',
                'speed': '⭐⭐⭐⭐⭐',
                'compression': '⭐⭐⭐',
                'use_case': 'General purpose, análisis interactivo'
            },
            'gzip': {
                'description': 'Compresión alta, velocidad moderada',
                'speed': '⭐⭐⭐',
                'compression': '⭐⭐⭐⭐⭐',
                'use_case': 'Archivado, transferencia de datos'
            },
            'brotli': {
                'description': 'Compresión muy alta, velocidad moderada',
                'speed': '⭐⭐',
                'compression': '⭐⭐⭐⭐⭐',
                'use_case': 'Máxima compresión, archivado'
            },
            'lz4': {
                'description': 'Muy rápido, compresión baja',
                'speed': '⭐⭐⭐⭐⭐',
                'compression': '⭐⭐',
                'use_case': 'Procesamiento en tiempo real'
            },
            'zstd': {
                'description': 'Balance óptimo velocidad/compresión',
                'speed': '⭐⭐⭐⭐',
                'compression': '⭐⭐⭐⭐',
                'use_case': 'Uso general moderno'
            },
            'none': {
                'description': 'Sin compresión',
                'speed': '⭐⭐⭐⭐⭐',
                'compression': '⭐',
                'use_case': 'Debugging, máxima velocidad'
            }
        }
        
        print(f"✅ Conversor inicializado con compresión: {compression.upper()}")
        print(f"📁 Directorio: {self.output_dir}")
        self.mostrar_info_compression(compression)

    def mostrar_info_compression(self, compression):
        """Muestra información sobre el tipo de compresión seleccionado"""
        if compression in self.compression_info:
            info = self.compression_info[compression]
            print(f"🗜️  Compresión {compression}:")
            print(f"   📝 {info['description']}")
            print(f"   ⚡ Velocidad: {info['speed']}")
            print(f"   📦 Compresión: {info['compression']}")
            print(f"   💡 Uso: {info['use_case']}")

    def comparar_compresiones(self, df_muestra, nombre_muestra="muestra"):
        """Compara diferentes tipos de compresión en una muestra"""
        print(f"\n🔬 COMPARANDO COMPRESIONES EN {nombre_muestra.upper()}")
        print("=" * 60)
        
        compresiones = ['none', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd']
        resultados = []
        
        temp_dir = Path("temp_compression_test")
        temp_dir.mkdir(exist_ok=True)
        
        for comp in compresiones:
            try:
                temp_file = temp_dir / f"test_{comp}.parquet"
                
                # Medir tiempo de escritura
                start_time = time.time()
                
                df_muestra.to_parquet(
                    temp_file,
                    compression=comp if comp != 'none' else None,
                    engine='pyarrow',
                    index=False
                )
                
                write_time = time.time() - start_time
                
                # Medir tiempo de lectura
                start_time = time.time()
                df_read = pd.read_parquet(temp_file)
                read_time = time.time() - start_time
                
                # Obtener tamaño
                file_size = temp_file.stat().st_size / (1024**2)  # MB
                
                resultados.append({
                    'compression': comp,
                    'size_mb': file_size,
                    'write_time': write_time,
                    'read_time': read_time,
                    'total_time': write_time + read_time
                })
                
                print(f"✅ {comp:8} | {file_size:6.2f}MB | W:{write_time:5.2f}s | R:{read_time:5.2f}s")
                
            except Exception as e:
                print(f"❌ {comp:8} | Error: {e}")
        
        # Limpiar archivos temporales
        for file in temp_dir.glob("*.parquet"):
            file.unlink()
        temp_dir.rmdir()
        
        # Mostrar recomendaciones
        if resultados:
            self.mostrar_recomendaciones(resultados)
        
        return resultados

    def mostrar_recomendaciones(self, resultados):
        """Muestra recomendaciones basadas en los resultados"""
        print(f"\n💡 RECOMENDACIONES:")
        print("-" * 30)
        
        # Ordenar por diferentes criterios
        by_size = sorted(resultados, key=lambda x: x['size_mb'])
        by_speed = sorted(resultados, key=lambda x: x['total_time'])
        
        print(f"🏆 Mejor compresión: {by_size[0]['compression']} ({by_size[0]['size_mb']:.2f}MB)")
        print(f"⚡ Más rápido: {by_speed[0]['compression']} ({by_speed[0]['total_time']:.2f}s)")
        
        # Recomendación balanceada
        balanced = min(resultados, key=lambda x: x['size_mb'] * x['total_time'])
        print(f"⚖️  Balance óptimo: {balanced['compression']}")

    def optimizar_dataframe(self, df):
        """Optimiza DataFrame para Parquet"""
        print(f"🔧 Optimizando DataFrame...")
        
        for col in df.columns:
            original_dtype = df[col].dtype
            
            # Category a string para compatibilidad
            if df[col].dtype.name == 'category':
                df[col] = df[col].astype('string')
                print(f"   📊 {col}: category -> string")
            
            # Optimizar enteros manteniendo compatibilidad
            elif 'int' in str(df[col].dtype):
                max_val = df[col].max()
                min_val = df[col].min()
                
                if min_val >= 0 and max_val < 2**8:
                    df[col] = df[col].astype('uint8')
                    print(f"   🔢 {col}: {original_dtype} -> uint8")
                elif min_val >= -2**7 and max_val < 2**7:
                    df[col] = df[col].astype('int8')
                    print(f"   🔢 {col}: {original_dtype} -> int8")
                elif min_val >= 0 and max_val < 2**16:
                    df[col] = df[col].astype('uint16')
                    print(f"   🔢 {col}: {original_dtype} -> uint16")
                elif min_val >= -2**15 and max_val < 2**15:
                    df[col] = df[col].astype('int16')
                    print(f"   🔢 {col}: {original_dtype} -> int16")
                elif min_val >= 0 and max_val < 2**32:
                    df[col] = df[col].astype('uint32')
                    print(f"   🔢 {col}: {original_dtype} -> uint32")
                else:
                    df[col] = df[col].astype('int64')
                    print(f"   🔢 {col}: {original_dtype} -> int64")
            
            # Optimizar flotantes
            elif 'float' in str(df[col].dtype):
                # Verificar si se puede usar float32 sin pérdida de precisión
                if df[col].max() < 3.4e38 and df[col].min() > -3.4e38:
                    df_temp = df[col].astype('float32')
                    if df_temp.equals(df[col].astype('float32')):
                        df[col] = df_temp
                        print(f"   💰 {col}: {original_dtype} -> float32")
                    else:
                        df[col] = df[col].astype('float64')
                        print(f"   💰 {col}: {original_dtype} -> float64")
            
            # Fechas
            elif 'datetime' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col])
                print(f"   📅 {col}: datetime64[ns]")
            
            # Objects a string
            elif df[col].dtype == 'object':
                if 'fecha' in col.lower():
                    try:
                        df[col] = pd.to_datetime(df[col])
                        print(f"   📅 {col}: object -> datetime64[ns]")
                    except:
                        df[col] = df[col].astype('string')
                        print(f"   📝 {col}: object -> string")
                else:
                    df[col] = df[col].astype('string')
                    print(f"   📝 {col}: object -> string")
        
        return df

    def crear_particiones_by_compression(self, df, dataset_type):
        """Crea particiones optimizadas por tipo de compresión"""
        print(f"📁 Creando particiones para {dataset_type} con {self.compression}...")
        
        particiones = []
        
        # Para compresiones que se benefician de datos similares juntos
        if self.compression in ['gzip', 'brotli', 'zstd']:
            # Agrupar datos similares para mejor compresión
            if dataset_type == 'ventas' and 'categoria' in df.columns:
                for categoria in sorted(df['categoria'].unique()):
                    df_part = df[df['categoria'] == categoria].copy()
                    if not df_part.empty:
                        particiones.append({
                            'data': df_part,
                            'path': f"categoria={categoria.lower().replace(' ', '_')}",
                            'name': f"ventas_cat_{categoria}"
                        })
            
            elif dataset_type == 'empleados' and 'departamento' in df.columns:
                for dept in sorted(df['departamento'].unique()):
                    df_part = df[df['departamento'] == dept].copy()
                    if not df_part.empty:
                        particiones.append({
                            'data': df_part,
                            'path': f"departamento={dept.lower().replace(' ', '_')}",
                            'name': f"empleados_dept_{dept}"
                        })
            
            elif dataset_type == 'marketing' and 'canal' in df.columns:
                for canal in sorted(df['canal'].unique()):
                    df_part = df[df['canal'] == canal].copy()
                    if not df_part.empty:
                        particiones.append({
                            'data': df_part,
                            'path': f"canal={canal.lower().replace(' ', '_')}",
                            'name': f"marketing_canal_{canal}"
                        })
        
        # Para compresiones rápidas, usar particiones más grandes
        else:
            # Particiones por tamaño óptimo (ej: 100k registros por archivo)
            chunk_size = 100000
            total_chunks = (len(df) + chunk_size - 1) // chunk_size
            
            for i in range(total_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, len(df))
                df_part = df.iloc[start_idx:end_idx].copy()
                
                particiones.append({
                    'data': df_part,
                    'path': f"chunk_{i:03d}",
                    'name': f"{dataset_type}_chunk_{i:03d}"
                })
        
        # Fallback: archivo único
        if not particiones:
            particiones.append({
                'data': df,
                'path': '',
                'name': dataset_type
            })
        
        print(f"   ✅ {len(particiones)} partición(es) creadas")
        return particiones

    def convertir_con_compression(self, csv_file, run_comparison=False):
        """Convierte CSV a Parquet con compresión específica"""
        print(f"\n🔄 Procesando: {csv_file}")
        print(f"🗜️  Compresión: {self.compression}")
        
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
            
            print(f"📊 Tipo: {dataset_type}")
            
            # Cargar CSV
            print(f"📖 Cargando CSV...")
            df = pd.read_csv(csv_file)
            print(f"   📈 {len(df):,} registros, {len(df.columns)} columnas")
            
            # Comparación opcional
            if run_comparison and len(df) > 1000:
                muestra = df.sample(min(10000, len(df)))
                self.comparar_compresiones(muestra, f"{dataset_type}_muestra")
            
            # Optimizar
            df = self.optimizar_dataframe(df)
            
            # Crear particiones
            particiones = self.crear_particiones_by_compression(df, dataset_type)
            
            # Directorio de salida
            output_dataset_dir = self.output_dir / f"{dataset_type}_{self.compression}"
            output_dataset_dir.mkdir(exist_ok=True)
            
            # Escribir archivos
            csv_size_mb = os.path.getsize(csv_file) / (1024**2)
            parquet_size_mb = 0
            archivos_generados = []
            
            total_start_time = time.time()
            
            for i, particion in enumerate(particiones):
                data = particion['data']
                path = particion['path']
                
                # Crear directorio
                if path:
                    full_path = output_dataset_dir / path
                    full_path.mkdir(parents=True, exist_ok=True)
                    parquet_file = full_path / f"data.parquet"
                else:
                    parquet_file = output_dataset_dir / f"{Path(csv_file).stem}.parquet"
                
                # Escribir con compresión específica
                start_time = time.time()
                
                try:
                    data.to_parquet(
                        parquet_file,
                        engine='pyarrow',
                        compression=self.compression if self.compression != 'none' else None,
                        index=False,
                        # Configuraciones adicionales para compresión
                        row_group_size=10000,  # Optimizar para compresión
                        data_page_size=1024*1024  # 1MB pages
                    )
                    
                    write_time = time.time() - start_time
                    file_size = parquet_file.stat().st_size / (1024**2)
                    parquet_size_mb += file_size
                    archivos_generados.append(str(parquet_file))
                    
                    print(f"   ✅ {parquet_file.name} ({len(data):,} reg, {file_size:.2f}MB, {write_time:.2f}s)")
                    
                except Exception as e:
                    print(f"   ❌ Error: {e}")
            
            total_time = time.time() - total_start_time
            
            # Metadata
            compression_ratio = ((csv_size_mb - parquet_size_mb) / csv_size_mb * 100) if csv_size_mb > 0 else 0
            
            metadata = {
                'conversion_info': {
                    'source_file': csv_file,
                    'dataset_type': dataset_type,
                    'compression': self.compression,
                    'created_at': datetime.now().isoformat(),
                    'total_time_seconds': round(total_time, 2)
                },
                'data_info': {
                    'total_records': len(df),
                    'total_columns': len(df.columns),
                    'partitions': len(particiones),
                    'files_generated': len(archivos_generados)
                },
                'size_info': {
                    'csv_size_mb': round(csv_size_mb, 2),
                    'parquet_size_mb': round(parquet_size_mb, 2),
                    'compression_ratio_percent': round(compression_ratio, 1),
                    'space_saved_mb': round(csv_size_mb - parquet_size_mb, 2)
                },
                'compression_details': self.compression_info.get(self.compression, {}),
                'files': archivos_generados
            }
            
            # Guardar metadata
            metadata_file = output_dataset_dir / f"metadata_{self.compression}.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            print(f"📄 Metadata: {metadata_file}")
            print(f"🗜️  Resultado: {csv_size_mb:.1f}MB → {parquet_size_mb:.1f}MB ({compression_ratio:.1f}%)")
            print(f"⏱️  Tiempo total: {total_time:.2f}s")
            
            return metadata
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            return None

    def convertir_todos_con_compression(self, comparar=False):
        """Convierte todos los CSVs con la compresión especificada"""
        print(f"🚀 CONVERSIÓN CSV → PARQUET ({self.compression.upper()})")
        print("=" * 50)
        
        csv_files = glob.glob("*.csv")
        if not csv_files:
            print("❌ No se encontraron archivos CSV")
            return []
        
        print(f"📁 Archivos a procesar: {len(csv_files)}")
        for csv_file in csv_files:
            size_mb = os.path.getsize(csv_file) / (1024**2)
            print(f"   📄 {csv_file} ({size_mb:.1f}MB)")
        
        resultados = []
        for csv_file in csv_files:
            resultado = self.convertir_con_compression(csv_file, run_comparison=comparar)
            if resultado:
                resultados.append(resultado)
        
        if resultados:
            self.crear_reporte_compression(resultados)
        
        return resultados

    def crear_reporte_compression(self, resultados):
        """Crea reporte específico de compresión"""
        print(f"\n📊 REPORTE COMPRESIÓN {self.compression.upper()}")
        print("=" * 40)
        
        total_csv = sum(r['size_info']['csv_size_mb'] for r in resultados)
        total_parquet = sum(r['size_info']['parquet_size_mb'] for r in resultados)
        total_time = sum(r['conversion_info']['total_time_seconds'] for r in resultados)
        total_files = sum(r['data_info']['files_generated'] for r in resultados)
        avg_compression = sum(r['size_info']['compression_ratio_percent'] for r in resultados) / len(resultados)
        
        print(f"✅ Archivos procesados: {len(resultados)}")
        print(f"📦 Tamaño original: {total_csv:.1f} MB")
        print(f"🗜️  Tamaño final: {total_parquet:.1f} MB")
        print(f"📉 Compresión promedio: {avg_compression:.1f}%")
        print(f"💾 Espacio ahorrado: {total_csv - total_parquet:.1f} MB")
        print(f"⏱️  Tiempo total: {total_time:.1f}s")
        print(f"📁 Archivos generados: {total_files}")
        
        # Velocidad de procesamiento
        total_records = sum(r['data_info']['total_records'] for r in resultados)
        records_per_second = total_records / total_time if total_time > 0 else 0
        print(f"⚡ Velocidad: {records_per_second:,.0f} registros/segundo")
        
        print(f"\n🔍 USO CON DIFERENTES HERRAMIENTAS:")
        print("# DuckDB")
        print(f"SELECT * FROM read_parquet('{self.output_dir}/**/*.parquet')")
        print("\n# Pandas")
        print(f"df = pd.read_parquet('{self.output_dir}')")
        print("\n# Spark")
        print(f"df = spark.read.parquet('{self.output_dir}')")

def main():
    """Función principal con selección de compresión"""
    print("🗜️  CONVERSOR CSV → PARQUET CON COMPRESIÓN")
    print("=" * 50)
    
    # Mostrar opciones de compresión
    print("📋 OPCIONES DE COMPRESIÓN DISPONIBLES:")
    print("-" * 40)
    converter_temp = ParquetCompressionConverter()
    for comp, info in converter_temp.compression_info.items():
        print(f"{comp:8} | {info['description']}")
        print(f"         | Velocidad: {info['speed']} | Compresión: {info['compression']}")
    
    print(f"\n¿Qué compresión quieres usar?")
    print("1. snappy (recomendado para uso general)")
    print("2. gzip (máxima compresión)")
    print("3. brotli (compresión premium)")
    print("4. lz4 (máxima velocidad)")
    print("5. zstd (balance moderno)")
    print("6. none (sin compresión)")
    print("7. comparar todas")
    
    try:
        choice = input("\nSelecciona (1-7): ").strip()
        
        compressions = {
            '1': 'snappy',
            '2': 'gzip', 
            '3': 'brotli',
            '4': 'lz4',
            '5': 'zstd',
            '6': 'none'
        }
        
        if choice == '7':
            # Comparar todas las compresiones
            for comp_name in ['snappy', 'gzip', 'brotli', 'lz4', 'zstd']:
                print(f"\n{'='*60}")
                print(f"PROCESANDO CON {comp_name.upper()}")
                print(f"{'='*60}")
                
                converter = ParquetCompressionConverter(
                    output_dir=f"parquet_{comp_name}",
                    compression=comp_name
                )
                converter.convertir_todos_con_compression()
        
        elif choice in compressions:
            compression = compressions[choice]
            converter = ParquetCompressionConverter(compression=compression)
            
            # Preguntar si quiere comparación
            compare = input("\n¿Comparar compresiones en una muestra? (y/n): ").lower().startswith('y')
            
            resultados = converter.convertir_todos_con_compression(comparar=compare)
            
            if resultados:
                print(f"\n🎉 ¡CONVERSIÓN COMPLETADA!")
                print(f"📁 Archivos guardados en: parquet_compressed/")
        else:
            print("❌ Opción inválida")
            
    except KeyboardInterrupt:
        print("\n👋 Cancelado por el usuario")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()