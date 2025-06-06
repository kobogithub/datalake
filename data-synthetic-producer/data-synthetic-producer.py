import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

class DatasetGeneratorFaker:
    def __init__(self, locale='es_ES'):
        """
        Inicializa el generador con Faker
        locale: 'es_ES' para español, 'en_US' para inglés
        """
        self.fake = Faker(locale)
        Faker.seed(42)  # Para reproducibilidad
        random.seed(42)
        print(f"✅ Generador Faker inicializado con locale: {locale}")

    def generar_dataset_ventas(self, registros=500):
        """Dataset 1: Ventas de e-commerce"""
        print(f"🛍️  Generando dataset de ventas ({registros} registros)...")
        
        categorias = {
            'Electrónicos': {
                'productos': ['Smartphone', 'Laptop', 'Tablet', 'Auriculares', 'Smart TV', 'Cámara Digital'],
                'precio_range': (50, 2000)
            },
            'Ropa': {
                'productos': ['Camiseta', 'Jeans', 'Vestido', 'Zapatos', 'Chaqueta', 'Falda'],
                'precio_range': (15, 300)
            },
            'Hogar': {
                'productos': ['Sofá', 'Mesa', 'Lámpara', 'Cojines', 'Cortinas', 'Espejo'],
                'precio_range': (20, 800)
            },
            'Deportes': {
                'productos': ['Zapatillas Running', 'Pelota Fútbol', 'Raqueta Tenis', 'Bicicleta', 'Pesas'],
                'precio_range': (25, 600)
            },
            'Libros': {
                'productos': ['Novela', 'Manual Técnico', 'Biografía', 'Comic', 'Libro Cocina'],
                'precio_range': (10, 80)
            }
        }
        
        metodos_pago = ['Tarjeta Crédito', 'Tarjeta Débito', 'PayPal', 'Transferencia', 'Efectivo']
        canales = ['Web', 'Móvil', 'Tienda Física']
        
        datos = []
        
        for i in range(registros):
            categoria = random.choice(list(categorias.keys()))
            producto = random.choice(categorias[categoria]['productos'])
            precio_min, precio_max = categorias[categoria]['precio_range']
            precio_unitario = round(random.uniform(precio_min, precio_max), 2)
            cantidad = random.randint(1, 5)
            descuento = round(random.uniform(0, 25), 1)
            total = round((precio_unitario * cantidad) * (1 - descuento/100), 2)
            
            registro = {
                'orden_id': f"ORD-{i+1:05d}",
                'fecha': self.fake.date_between(start_date='-12M', end_date='today'),
                'cliente_id': f"CUST-{random.randint(1, 1000):05d}",
                'producto': producto,
                'categoria': categoria,
                'precio_unitario': precio_unitario,
                'cantidad': cantidad,
                'descuento_porcentaje': descuento,
                'total': total,
                'metodo_pago': random.choice(metodos_pago),
                'ciudad': self.fake.city(),
                'pais': self.fake.country(),
                'edad_cliente': random.randint(18, 70),
                'genero': random.choice(['M', 'F']),
                'canal': random.choice(canales),
                'tiempo_envio_dias': random.randint(1, 7)
            }
            datos.append(registro)
        
        return datos

    def generar_dataset_empleados(self, registros=200):
        """Dataset 2: Empleados para análisis de RRHH"""
        print(f"👥 Generando dataset de empleados ({registros} registros)...")
        
        departamentos = {
            'IT': ['Desarrollador', 'Analista de Sistemas', 'DevOps', 'QA Tester', 'Arquitecto de Software'],
            'Marketing': ['Especialista en Marketing', 'Community Manager', 'Analista de Marketing', 'SEO Specialist'],
            'Ventas': ['Ejecutivo de Ventas', 'Account Manager', 'Business Developer', 'Vendedor'],
            'RRHH': ['Reclutador', 'Analista de RRHH', 'Especialista en Capacitación', 'HR Business Partner'],
            'Finanzas': ['Contador', 'Analista Financiero', 'Tesorero', 'Auditor'],
            'Operaciones': ['Supervisor de Operaciones', 'Coordinador Logística', 'Analista de Procesos']
        }
        
        niveles = ['Junior', 'Semi-Senior', 'Senior', 'Lead', 'Manager', 'Director']
        educacion = ['Secundaria', 'Técnico', 'Universitario', 'Postgrado', 'Maestría', 'Doctorado']
        estados = ['Activo', 'Licencia', 'Vacaciones']
        ubicaciones = ['Oficina Central', 'Remoto', 'Híbrido']
        
        datos = []
        
        for i in range(registros):
            departamento = random.choice(list(departamentos.keys()))
            cargo = random.choice(departamentos[departamento])
            años_exp = random.randint(0, 25)
            edad = random.randint(22, 65)
            
            # Salario basado en experiencia y nivel
            salario_base = {
                'Junior': 35000, 'Semi-Senior': 55000, 'Senior': 75000,
                'Lead': 95000, 'Manager': 120000, 'Director': 150000
            }
            nivel = random.choice(niveles)
            salario = salario_base[nivel] + (años_exp * 2000) + random.randint(-10000, 15000)
            salario = max(30000, salario)  # Salario mínimo
            
            # Performance correlacionado con experiencia
            performance_base = min(5.0, 2.0 + (años_exp * 0.1) + random.uniform(-0.5, 0.5))
            performance = round(max(1.0, performance_base), 1)
            
            registro = {
                'empleado_id': f"EMP-{i+1:05d}",
                'nombre': self.fake.first_name(),
                'apellido': self.fake.last_name(),
                'edad': edad,
                'genero': random.choice(['M', 'F']),
                'departamento': departamento,
                'cargo': cargo,
                'nivel': nivel,
                'salario_anual': salario,
                'fecha_ingreso': self.fake.date_between(start_date='-10y', end_date='today'),
                'educacion': random.choice(educacion),
                'años_experiencia': años_exp,
                'performance_score': performance,
                'horas_extra_mes': random.randint(0, 40),
                'proyectos_completados': random.randint(0, 50),
                'capacitaciones_año': random.randint(0, 12),
                'estado': random.choice(estados),
                'ubicacion': random.choice(ubicaciones),
                'satisfaccion_laboral': random.randint(1, 10)
            }
            datos.append(registro)
        
        return datos

    def generar_dataset_marketing(self, registros=300):
        """Dataset 3: Campañas de marketing digital"""
        print(f"📈 Generando dataset de marketing ({registros} registros)...")
        
        canales = ['Facebook', 'Google Ads', 'Instagram', 'LinkedIn', 'Email', 'YouTube', 'TikTok']
        tipos_campaña = ['Display', 'Video', 'Search', 'Social', 'Email', 'Influencer']
        audiencias = ['18-25', '26-35', '36-45', '46-55', '55+']
        industrias = ['Retail', 'Tech', 'Salud', 'Educación', 'Finanzas', 'Entretenimiento']
        
        datos = []
        
        for i in range(registros):
            presupuesto = round(random.uniform(500, 50000), 2)
            gasto_real = round(presupuesto * random.uniform(0.8, 1.0), 2)  # 80-100% del presupuesto
            impresiones = random.randint(1000, 1000000)
            
            # Métricas realistas basadas en industria
            ctr_base = random.uniform(0.5, 8.0)  # CTR realista
            clics = int(impresiones * (ctr_base / 100))
            
            conversion_rate = random.uniform(1, 15)  # 1-15% conversion rate
            conversiones = int(clics * (conversion_rate / 100))
            
            cpc = round(gasto_real / clics if clics > 0 else 0, 2)
            cpm = round((gasto_real / impresiones) * 1000, 2)
            
            # ROAS (Return on Ad Spend)
            roas = round(random.uniform(0.5, 8.0), 2)
            ventas_generadas = int(conversiones * random.uniform(0.3, 0.8))
            
            registro = {
                'campaña_id': f"CAMP-{i+1:05d}",
                'nombre_campaña': f"Campaña {self.fake.catch_phrase()}",
                'fecha_inicio': self.fake.date_between(start_date='-24M', end_date='-1M'),
                'fecha_fin': self.fake.date_between(start_date='-1M', end_date='today'),
                'canal': random.choice(canales),
                'tipo_campaña': random.choice(tipos_campaña),
                'presupuesto': presupuesto,
                'gasto_real': gasto_real,
                'impresiones': impresiones,
                'clics': clics,
                'conversiones': conversiones,
                'ventas_generadas': ventas_generadas,
                'ctr': round(ctr_base, 2),
                'cpc': cpc,
                'cpm': cpm,
                'roas': roas,
                'audiencia_objetivo': random.choice(audiencias),
                'genero_objetivo': random.choice(['M', 'F', 'Ambos']),
                'ubicacion': self.fake.country(),
                'industria': random.choice(industrias)
            }
            datos.append(registro)
        
        return datos

    def guardar_csv(self, datos, nombre_archivo):
        """Guarda los datos como CSV"""
        if not datos:
            print(f"❌ No hay datos para guardar en {nombre_archivo}")
            return None
            
        df = pd.DataFrame(datos)
        archivo_csv = f"{nombre_archivo}.csv"
        
        try:
            df.to_csv(archivo_csv, index=False, encoding='utf-8')
            print(f"✅ Guardado: {archivo_csv} ({len(datos)} registros)")
            print(f"   📊 Columnas: {list(df.columns)}")
            print(f"   📏 Tamaño: {df.shape[0]} filas x {df.shape[1]} columnas")
            
            return df
        except Exception as e:
            print(f"❌ Error al guardar {archivo_csv}: {e}")
            return None

    def generar_reporte_resumen(self, datasets_info):
        """Genera un reporte resumen de los datasets"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        reporte_file = f"reporte_datasets_{timestamp}.txt"
        
        with open(reporte_file, 'w', encoding='utf-8') as f:
            f.write("="*60 + "\n")
            f.write("REPORTE DE DATASETS GENERADOS\n")
            f.write("="*60 + "\n")
            f.write(f"Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for dataset in datasets_info:
                f.write(f"📁 {dataset['nombre']}:\n")
                f.write(f"   📄 Archivo: {dataset['archivo']}\n")
                f.write(f"   📊 Registros: {dataset['registros']}\n")
                f.write(f"   🔢 Columnas: {len(dataset['columnas'])}\n")
                f.write(f"   📋 Campos: {', '.join(dataset['columnas'])}\n\n")
            
            f.write("🔍 ANÁLISIS SUGERIDOS:\n")
            f.write("-"*30 + "\n")
            f.write("📈 Ventas:\n")
            f.write("  • Análisis temporal de ventas por categoría\n")
            f.write("  • Segmentación de clientes por edad y género\n")
            f.write("  • Efectividad de descuentos vs volumen de ventas\n")
            f.write("  • Análisis de canales de venta más rentables\n\n")
            
            f.write("👥 Empleados:\n")
            f.write("  • Correlación salario vs performance\n")
            f.write("  • Análisis de satisfacción por departamento\n")
            f.write("  • Distribución de niveles y experiencia\n")
            f.write("  • Predicción de rotación de personal\n\n")
            
            f.write("📊 Marketing:\n")
            f.write("  • ROI por canal de marketing\n")
            f.write("  • Optimización de presupuestos\n")
            f.write("  • Análisis de CTR y conversion rates\n")
            f.write("  • Segmentación de audiencias más efectivas\n")
        
        print(f"📄 Reporte guardado: {reporte_file}")

    def generar_todos_los_datasets(self):
        """Genera los 3 datasets completos con Faker"""
        print("🚀 Iniciando generación de datasets con Faker...")
        print("=" * 60)
        
        datasets_info = []
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        
        # Dataset 1: Ventas
        ventas = self.generar_dataset_ventas(100000)
        df_ventas = self.guardar_csv(ventas, f"ventas_ecommerce_{timestamp}")
        if df_ventas is not None:
            datasets_info.append({
                'nombre': 'Ventas E-commerce',
                'archivo': f"ventas_ecommerce_{timestamp}.csv",
                'registros': len(ventas),
                'columnas': list(df_ventas.columns)
            })
        
        print()
        
        # Dataset 2: Empleados
        empleados = self.generar_dataset_empleados(100000)
        df_empleados = self.guardar_csv(empleados, f"empleados_rrhh_{timestamp}")
        if df_empleados is not None:
            datasets_info.append({
                'nombre': 'Empleados RRHH',
                'archivo': f"empleados_rrhh_{timestamp}.csv",
                'registros': len(empleados),
                'columnas': list(df_empleados.columns)
            })
        
        print()
        
        # Dataset 3: Marketing
        marketing = self.generar_dataset_marketing(100000)
        df_marketing = self.guardar_csv(marketing, f"campañas_marketing_{timestamp}")
        if df_marketing is not None:
            datasets_info.append({
                'nombre': 'Campañas Marketing',
                'archivo': f"campañas_marketing_{timestamp}.csv",
                'registros': len(marketing),
                'columnas': list(df_marketing.columns)
            })
        
        # Generar reporte
        if datasets_info:
            self.generar_reporte_resumen(datasets_info)
        
        # Resumen final
        print("\n" + "=" * 60)
        print("🎉 ¡GENERACIÓN COMPLETADA!")
        print("=" * 60)
        
        total_registros = sum(d['registros'] for d in datasets_info)
        print(f"📊 Total de registros generados: {total_registros:,}")
        
        for dataset in datasets_info:
            print(f"\n📁 {dataset['nombre']}:")
            print(f"   📄 {dataset['archivo']}")
            print(f"   📊 {dataset['registros']} registros")
        
        print(f"\n💡 Todos los archivos están listos para análisis con pandas!")
        print(f"🕐 Proceso completado en: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return datasets_info

def main():
    """Función principal"""
    try:
        print("🐍 Generador de Datasets con Faker")
        print("=" * 40)
        
        # Crear generador (puedes cambiar el locale)
        generador = DatasetGeneratorFaker(locale='es_ES')  # Cambia a 'en_US' si prefieres inglés
        
        # Generar todos los datasets
        info = generador.generar_todos_los_datasets()
        
        # Ejemplo de código para análisis
        print(f"\n" + "🔍 CÓDIGO PARA ANÁLISIS:" + "="*30)
        print("import pandas as pd")
        print("import matplotlib.pyplot as plt")
        print("import seaborn as sns")
        print()
        if info:
            for dataset in info:
                var_name = dataset['archivo'].split('_')[0]
                print(f"# Cargar {dataset['nombre']}")
                print(f"{var_name} = pd.read_csv('{dataset['archivo']}')")
                print(f"print({var_name}.head())")
                print(f"print({var_name}.info())")
                print()
        
    except ImportError:
        print("❌ Error: Instala las dependencias necesarias:")
        print("   pip install faker pandas")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")

if __name__ == "__main__":
    main()