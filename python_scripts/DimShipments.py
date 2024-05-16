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
            UPDATE "Dim"."Shipment" AS s
            SET 
                "ShipmentPackageStatus" = ss."ShipmentPackageStatus",
                "AgreedDeliveryDate" = ss."AgreedDeliveryDate",
                "OriginShipmentDate" = ss."OriginShipmentDate",
                "EstimatedDeliveryStartDate" = ss."EstimatedDeliveryStartDate",
                "EstimatedDeliveryEndDate" = ss."EstimatedDeliveryEndDate",
                "AgreedDeliveryExtensionStartDate" = ss."AgreedDeliveryExtensionStartDate",
                "AgreedDeliveryExtensionEndDate" = ss."AgreedDeliveryExtensionEndDate"
            FROM "stage"."Shipment" AS ss
            WHERE s."OrderId" = ss."OrderId";
            
            Insert Into "Dim"."Shipment"
            SELECT "stage"."Shipment".* 
            FROM "stage"."Shipment"
            LEFT JOIN "Dim"."Shipment" ON "stage"."Shipment"."OrderId" = "Dim"."Shipment"."OrderId"
            where "Dim"."Shipment"."OrderId" is NULL;
            """)

        # Değişiklikleri kaydet ve bağlantıyı kapat
        pg_connection.commit()
        print("DimShipments Data transfer completed.")

    except psycopg2.Error as e:
        print("DimShipments Error transferring data:", e)

    finally:
        pg_cursor.close()
        pg_connection.close()

# Veri aktarımını başlat
transfer_data()
