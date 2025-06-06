import openai
import json
import os

# Configuración básica con variable de entorno
api_key = os.getenv('OPENAI_API_KEY')
if not api_key:
    raise ValueError("Por favor configura la variable de entorno OPENAI_API_KEY")

client = openai.OpenAI(api_key=api_key)

def generar_datos_simples():
    """Ejemplo básico para generar datos sintéticos"""
    
    prompt = """
    Genera 10 empleados ficticios en formato JSON con esta estructura:
    {
        "nombre": "string",
        "departamento": "string", 
        "salario": "number",
        "años_experiencia": "number",
        "email": "string"
    }
    
    Devuelve solo el array JSON válido.
    """
    
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "Eres un generador de datos sintéticos. Responde solo con JSON válido."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.8,
        max_tokens=2000
    )
    
    try:
        # Extraer y parsear la respuesta
        contenido = response.choices[0].message.content
        print("Respuesta recibida:")
        print(contenido)
        
        # Intentar parsear como JSON
        datos = json.loads(contenido)
        return datos
        
    except json.JSONDecodeError as e:
        print(f"Error al parsear JSON: {e}")
        return None

# Ejecutar el ejemplo
if __name__ == "__main__":
    print("🚀 Iniciando generación de datos sintéticos...")
    print(f"📍 API Key cargada: {'✅ Sí' if api_key else '❌ No'}")
    
    empleados = generar_datos_simples()
    
    if empleados:
        print(f"\n✅ Se generaron {len(empleados)} empleados sintéticos:")
        for i, emp in enumerate(empleados[:3], 1):  # Mostrar solo los primeros 3
            print(f"{i}. {emp['nombre']} - {emp['departamento']} - ${emp['salario']}")
        
        # Guardar en archivo JSON
        with open('empleados_sinteticos.json', 'w', encoding='utf-8') as f:
            json.dump(empleados, f, indent=2, ensure_ascii=False)
        print("\n💾 Datos guardados en 'empleados_sinteticos.json'")
    else:
        print("❌ No se pudieron generar los datos")