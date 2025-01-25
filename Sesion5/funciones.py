def transform_order_items(df, orders_df, products_df):
    """
    Realiza transformaciones específicas en el DataFrame de order_items.
    """
    # Asegurar que order_item_order_id exista en orders
    valid_order_ids = set(orders_df['order_id'])
    if not df['order_item_order_id'].isin(valid_order_ids).all():
        logging.error("Hay order_item_order_id que no existen en orders.")
        sys.exit(1)
    # Asegurar que order_item_product_id exista en products
    valid_product_ids = set(products_df['product_id'])
    if not df['order_item_product_id'].isin(valid_product_ids).all():
        logging.error("Hay order_item_product_id que no existen en products.")
        sys.exit(1)
    # Calcular order_item_subtotal si no está presente o está incorrecto
    calculated_subtotal = df['order_item_quantity'] * df['order_item_product_price']
    if not (df['order_item_subtotal'] == calculated_subtotal).all():
        logging.info("Recalculando order_item_subtotal.")
        df['order_item_subtotal'] = calculated_subtotal
    return df

def load_data(engine, table_name, df, if_exists='append'):
    """
    Carga un DataFrame de pandas a una tabla de MySQL.
    """
    try:
        df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)
        logging.info(f"Datos cargados exitosamente en la tabla {table_name}.")
    except Exception as e:
        logging.error(f"Error al cargar datos en la tabla {table_name}: {e}")
        sys.exit(1)