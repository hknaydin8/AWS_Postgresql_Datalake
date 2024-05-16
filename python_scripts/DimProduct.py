import psycopg2
from configparser import ConfigParser
import pandas as pd
import json

config = ConfigParser()
config.read('db_config.ini')

# PostgreSQL bağlantı bilgilerini alıyoruz.
postgresql_environment = 'dbinstance'
postgresql_server = config.get(postgresql_environment, 'server')
postgresql_username = config.get(postgresql_environment, 'user')
postgresql_password = config.get(postgresql_environment, 'password')
postgresql_database = config.get(postgresql_environment, 'database')

# PostgreSQL bağlantısı
pg_connection = psycopg2.connect(
    host=postgresql_server,
    port="5432",
    database=postgresql_database,
    user=postgresql_username,
    password=postgresql_password
)
pg_cursor = pg_connection.cursor()

def transfer_data():
    try:
        pg_cursor.execute("""
            TRUNCATE TABLE "Dim"."Product";

            Insert Into "Dim"."Product"
            SELECT "ProductCode","ProductSku","ProductBarcode","Price","ProductSize","ProductColor","ProductOrigin","Amount"
            FROM "stage"."Product";
        """)

        # Değişiklikleri kaydet ve bağlantıyı kapat
        pg_connection.commit()
        print("DimProducts Data transfer completed.")

    except psycopg2.Error as e:
        print("DimProducts Error transferring data:", e)

    finally:
        pg_cursor.close()
        pg_connection.close()

# Veri aktarımını başlat
transfer_data()
