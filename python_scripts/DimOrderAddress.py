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
            UPDATE "Dim"."OrderAddress" AS oa
            SET 
                "OrderAddressType" = soa."OrderAddressType",
                "Company" = soa."Company",
                "Address1" = soa."Address1",
                "Address2" = soa."Address2",
                "CityCode" = soa."CityCode",
                "City" = soa."City",
                "DistrictId" = soa."DistrictId",
                "District" = soa."District",
                "PostalCode" = soa."PostalCode",
                "CountryCode" = soa."CountryCode",
                "FullAddress" = soa."FullAddress",
                "NeighborhoodId" = soa."NeighborhoodId",
                "Neighborhood" = soa."Neighborhood"
            FROM "stage"."OrderAddress" AS soa
            WHERE oa."Id" = soa."Id"
                AND oa."OrderId" = soa."OrderId";
            
            
            INSERT INTO "Dim"."OrderAddress"
            SELECT soa.* 
            FROM "stage"."OrderAddress" AS soa
            LEFT JOIN "Dim"."OrderAddress" AS oa 
            ON soa."Id" = oa."Id" AND soa."OrderId" = oa."OrderId"
            WHERE oa."Id" IS NULL;
        """)

        # Değişiklikleri kaydet ve bağlantıyı kapat
        pg_connection.commit()
        print("DimOrderAddress Data transfer completed.")

    except psycopg2.Error as e:
        print("DimOrderAddress Error transferring data:", e)

    finally:
        pg_cursor.close()
        pg_connection.close()

# Veri aktarımını başlat
transfer_data()
