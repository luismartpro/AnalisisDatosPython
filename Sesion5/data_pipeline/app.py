import pandas as pd
from sqlalchemy import create_engine
import logging
import sys
from config import DATABASE_CONFIG, CSV_FILES, LOGS_FILES

#configuracion del loggin
# estructura del loggin = fecha - info - mensaje
logging.basicConfig(
    filename=LOGS_FILES,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_db_engine(config):
    """
    crear una conexion a la base de datos de Mysql.
    """
    try:
        engine = create_engine(
            f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
        logging.info("conexion establecida a la base de datos")
        return engine
    except Exception as e:
        logging.error(f"Error en la base de datos {e}")
        sys.exit(1)

def read_csv(file_path):
    """
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Archivo  {file_path} leido correctamente")
        return df
    except Exception as e:
        logging.error(f"Error al leer el archivo {file_path}")
        sys.exit(1)
    

def transform_departmets(df):
    """
    """
    if  df['department_name'].duplicated().any():
        logging.warning("Hay departamentos duplicados")
    return df

def transform_customers(df):
    """
    """

    df['customer_email'] = df['customer_email'].str.lower()
    
    if df[['customer_fname', 'customer_lname', 'customer_email']].isnull().any().any():
        logging.error('Datos faltantes en el dataframe de customers')
        sys.exit(1)

    return df

def transform_categories(df, department_df):
    """
    """

    valid_ids = set(department_df['department_id'])
    if not df['category_department_id'].isin(valid_ids).all():
        logging.error("Hay category_department_id que no existen en el departments")
        sys.exit(1)
    return df

def transform_product(df, categories_df):
    """
    """

    valid_ids = set(categories_df['category_id'])
    if not df['product_category_id'].isin(valid_ids).all():
        logging.error("Hay product_category_id que no existen en el category")
        sys.exit(1)
    return df

def transform_order(df, customer_df):
    """
    """

    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    if  df['order_date'].isnull().any():
        logging.error("Hay valores invalidos en el order_date")
        sys.exit(1)

    valid_ids = set(customer_df['customer_id'])
    if not df['order_customer_id'].isin(valid_ids).all():
        logging.error("Hay order_customer_id que no existen en el customers")
        sys.exit(1)

    return df

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

###-----------------------------------------------
def main():
    engine = create_db_engine(DATABASE_CONFIG)

    departments_df = read_csv(CSV_FILES['departments'])
    departments_df = transform_departmets(departments_df)

    customers_df = read_csv(CSV_FILES['customers'])
    customers_df = transform_customers(customers_df)

    categories_df = read_csv(CSV_FILES['categories'])
    categories_df = transform_categories(categories_df, departments_df)

    products_df = read_csv(CSV_FILES['products'])
    products_df = transform_product(products_df, categories_df)

    oders_df = read_csv(CSV_FILES['orders'])
    oders_df = transform_order(oders_df, customers_df)

    oder_items_df = read_csv(CSV_FILES['order_items'])
    oder_items_df = transform_order_items(oder_items_df, oders_df, products_df)

if __name__ == "__main__":
    main()



    