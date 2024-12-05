import pandas as pd
import numpy as np

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Read the CSV file into a DataFrame
df = pd.read_csv('sampled_products.csv')

# Generate random days in 2024 for each row
df['marca_de_tiempo'] = df.apply(lambda _: pd.to_datetime(np.random.choice(pd.date_range('2024-01-01', '2024-12-31'))), axis=1)

# Convert to string with the desired format 'yyyy-mm-dd 00:00:00.000'
df['marca_de_tiempo'] = df['marca_de_tiempo'].dt.strftime('%Y-%m-%d 00:00:00.000')

# Sample 10000 rows with replacement
sampled_df = df.sample(n=10000, replace=True)

# Dictionary to map English column names to Spanish
column_mapping = {
    'timestamp': 'marca_de_tiempo',
    'url': 'url',
    'final_price': 'precio_final',
    'sku': 'sku',
    'currency': 'moneda',
    'gtin': 'gtin',
    'specifications': 'especificaciones',
    'image_urls': 'urls_imagenes',
    'top_reviews': 'mejores_comentarios',
    'rating_stars': 'estrellas_calificacion',
    'related_pages': 'paginas_relacionadas',
    'available_for_delivery': 'disponible_para_entrega',
    'available_for_pickup': 'disponible_para_recogida',
    'brand': 'marca',
    'breadcrumbs': 'migajas_pan',
    'category_ids': 'ids_categoria',
    'review_count': 'cantidad_comentarios',
    'description': 'descripcion',
    'product_id': 'id_producto',
    'product_name': 'nombre_producto',
    'review_tags': 'etiquetas_de_revision',
    'category_url': 'url_categoria',
    'category_name': 'nombre_categoria',
    'category_path': 'ruta_categoria',
    'root_category_url': 'url_categoria_raiz',
    'root_category_name': 'nombre_categoria_raiz',
    'upc': 'upc',
    'tags': 'etiquetas',
    'main_image': 'imagen_principal',
    'rating': 'calificacion',
    'unit_price': 'precio_unitario',
    'unit': 'unidad',
    'aisle': 'pasillo',
    'free_returns': 'devoluciones_gratuitas',
    'sizes': 'tallas',
    'colors': 'colores',
    'seller': 'vendedor',
    'other_attributes': 'otros_atributos',
    'customer_reviews': 'comentarios_clientes',
    'ingredients': 'ingredientes',
    'initial_price': 'precio_inicial',
    'discount': 'descuento',
    'ingredients_full': 'ingredientes_completo',
    'categories': 'categorias'
}

# Rename the columns
sampled_df = sampled_df.rename(columns=column_mapping)

# Save the dataframe to a file named "sampled_products.csv"
sampled_df.to_csv("csv_expandido.csv", index=False)