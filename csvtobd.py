import pandas as pd
import json
from sqlalchemy import create_engine


# Cargar el CSV
csv_path = 'Expanded_Walmart_Products_Data_Spanish.csv'
df = pd.read_csv(csv_path)

# Convertir la columna de fecha a un formato adecuado
df['fecha'] = pd.to_datetime(df['marca_de_tiempo'], errors='coerce')
df = df.dropna(subset=['fecha'])  # Eliminar filas donde la fecha es NaT

# Extraer día, mes y año
df['dia'] = df['fecha'].dt.day
df['mes'] = df['fecha'].dt.month
df['anio'] = df['fecha'].dt.year

# Función para convertir JSON o listas en diccionarios legibles
def parse_json_column(column):
    parsed_column = []
    for item in column:
        if pd.isnull(item):
            parsed_column.append({})
        elif isinstance(item, str):
            try:
                parsed_column.append(json.loads(item.replace("'", "\"")))
            except json.JSONDecodeError:
                parsed_column.append({})
        elif isinstance(item, list) and len(item) > 0:
            try:
                parsed_column.append(json.loads(item[0].replace("'", "\"")) if isinstance(item[0], str) else item[0])
            except (json.JSONDecodeError, TypeError):
                parsed_column.append({})
        elif isinstance(item, dict):
            parsed_column.append(item)
        else:
            parsed_column.append({})
    return pd.Series(parsed_column)

# Procesar la columna 'estrellas_calificacion' si es JSON o lista
df['estrellas_calificacion'] = parse_json_column(df['estrellas_calificacion'])

# Extraer las calificaciones de cada tipo de estrella
def get_star_value(data, star_type):
    if isinstance(data, dict):
        return data.get(star_type, 0)
    elif isinstance(data, list) and len(data) > 0:
        return data[0].get(star_type, 0) if isinstance(data[0], dict) else 0
    return 0

df['cinco_estrellas'] = df['estrellas_calificacion'].apply(lambda x: get_star_value(x, 'five_stars'))
df['cuatro_estrellas'] = df['estrellas_calificacion'].apply(lambda x: get_star_value(x, 'four_stars'))
df['tres_estrellas'] = df['estrellas_calificacion'].apply(lambda x: get_star_value(x, 'three_stars'))
df['dos_estrellas'] = df['estrellas_calificacion'].apply(lambda x: get_star_value(x, 'two_stars'))
df['una_estrella'] = df['estrellas_calificacion'].apply(lambda x: get_star_value(x, 'one_star'))

# Procesar la columna 'mejores_comentarios' y extraer comentarios positivos y negativos
df['mejores_comentarios'] = parse_json_column(df['mejores_comentarios'])

# Extraer comentarios positivos y negativos
df['comentarios_positivos'] = df['mejores_comentarios'].apply(lambda x: x.get('positive', {}).get('review', '') if isinstance(x, dict) else '')
df['comentarios_negativos'] = df['mejores_comentarios'].apply(lambda x: x.get('negative', {}).get('review', '') if isinstance(x, dict) else '')

# Procesar la columna 'comentarios_clientes' si es JSON
df['comentarios_clientes'] = parse_json_column(df['comentarios_clientes'])
df['comentarios_de_clientes'] = df['comentarios_clientes'].apply(lambda x: [comment['review'] for comment in x if 'review' in comment] if isinstance(x, list) else '')

# Convertir listas en 'dim_calificaciones' a cadenas de texto
def list_to_string(value):
    if isinstance(value, list):
        return ', '.join(map(str, value))
    return value

df['comentarios_positivos'] = df['comentarios_positivos'].apply(list_to_string)
df['comentarios_negativos'] = df['comentarios_negativos'].apply(list_to_string)
df['comentarios_de_clientes'] = df['comentarios_de_clientes'].apply(list_to_string)

# Crear dimensiones
# Dimensión Producto
dim_producto = df[['sku', 'nombre_producto', 'marca', 'descripcion', 'especificaciones', 'urls_imagenes', 'unidad', 'tallas', 'colores', 'ingredientes', 'ingredientes_completo', 'otros_atributos', 'imagen_principal']].drop_duplicates().reset_index(drop=True)
dim_producto.columns = ['id_producto', 'nombre_producto', 'marca', 'descripcion', 'especificaciones', 'urls_imagenes', 'unidad', 'tallas', 'colores', 'ingredientes', 'ingredientes_completo', 'otros_atributos', 'imagen_principal']
dim_producto.insert(0, 'id_producto_dim', range(1, 1 + len(dim_producto)))

# Dimensión Marca
dim_marca = df[['marca']].drop_duplicates().reset_index(drop=True)
dim_marca.columns = ['nombre_marca']
dim_marca.insert(0, 'id_marca', range(1, 1 + len(dim_marca)))

# Dimensión Categoría
dim_categoria = df[['ids_categoria', 'nombre_categoria', 'migajas_pan', 'url_categoria', 'ruta_categoria', 'url_categoria_raiz', 'nombre_categoria_raiz']].drop_duplicates().reset_index(drop=True)
dim_categoria.columns = ['id_categoria', 'nombre_categoria', 'ruta_categoria', 'url_categoria', 'url_categoria_raiz', 'nombre_categoria_raiz', 'migajas_pan']
dim_categoria.insert(0, 'id_categoria_dim', range(1, 1 + len(dim_categoria)))

# Dimensión Tiempo (Asegurar que no haya fechas con valores NULL)
dim_tiempo = df[['fecha', 'dia', 'mes', 'anio']].dropna().reset_index(drop=True)
dim_tiempo.columns = ['fecha', 'dia', 'mes', 'anio']
dim_tiempo.insert(0, 'id_tiempo_dim', range(1, 1 + len(dim_tiempo)))

# Dimensión Calificaciones sin la columna 'tags'
dim_calificaciones = df[['cinco_estrellas', 'cuatro_estrellas', 'tres_estrellas', 'dos_estrellas', 'una_estrella', 
                         'comentarios_positivos', 'comentarios_negativos', 'comentarios_de_clientes']].drop_duplicates().reset_index(drop=True)
dim_calificaciones.insert(0, 'id_calificaciones_dim', range(1, 1 + len(dim_calificaciones)))

# Crear tabla de hechos
hechos_productos = df[['sku', 'ids_categoria', 'fecha', 'precio_final', 'precio_unitario', 'precio_inicial', 'descuento', 'cantidad_comentarios', 'disponible_para_entrega', 'disponible_para_recogida']].reset_index(drop=True)
hechos_productos.columns = ['id_producto', 'id_categoria', 'fecha', 'precio_final', 'precio_unitario', 'precio_inicial', 'descuento', 'cantidad_comentarios', 'disponible_para_entrega', 'disponible_para_recogida']

# Reemplazar IDs originales con IDs de las dimensiones
hechos_productos = hechos_productos.merge(dim_producto[['id_producto', 'id_producto_dim']], on='id_producto')
hechos_productos = hechos_productos.merge(dim_categoria[['id_categoria', 'id_categoria_dim']], on='id_categoria')
hechos_productos = hechos_productos.merge(dim_tiempo[['fecha', 'id_tiempo_dim']], on='fecha')
hechos_productos = hechos_productos.merge(dim_calificaciones[['id_calificaciones_dim']], left_index=True, right_index=True)

# Seleccionar solo las columnas necesarias para la tabla de hechos
hechos_productos = hechos_productos[['id_producto_dim', 'id_categoria_dim', 'id_tiempo_dim', 'id_calificaciones_dim', 'precio_final', 'precio_unitario', 'precio_inicial', 'descuento', 'cantidad_comentarios', 'disponible_para_entrega', 'disponible_para_recogida']]
hechos_productos.insert(0, 'id_hecho_producto', range(1, 1 + len(hechos_productos)))

# Conectar a la base de datos MySQL
engine = create_engine('')


# Guardar dimensiones en la base de datos con nombres en español
dim_producto.to_sql('dim_producto', con=engine, if_exists='replace', index=False)
dim_marca.to_sql('dim_marca', con=engine, if_exists='replace', index=False)
dim_categoria.to_sql('dim_categoria', con=engine, if_exists='replace', index=False)
dim_tiempo.to_sql('dim_tiempo', con=engine, if_exists='replace', index=False)
dim_calificaciones.to_sql('dim_calificaciones', con=engine, if_exists='replace', index=False)
hechos_productos.to_sql('hechos_productos', con=engine, if_exists='replace', index=False)

print("Modelo estrella cargado y datos del CSV guardados correctamente en la base de datos.")

