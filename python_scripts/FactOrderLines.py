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
            DROP TABLE IF EXISTS stg_OrderLines;
            CREATE TEMP TABLE stg_OrderLines AS
            SELECT Cast("OrderDate" as Date) "OrderDate", "OrderId", "OrderLineId", "MerchantId", "ProductCode", "CurrencyCode","UnitPrice",
                SUM("Quantity") "OrderLineTotalQuantity",
                SUM("lineItemPrice") "OrderLineTotalItemPrice",
                SUM("lineItemDiscount") "OrderLineTotalItemDiscount",
                SUM("lineItemTyDiscount") "OrderLineTotalItemTyDiscount",
                SUM(("lineItemPrice"*"Quantity")-("Quantity"*"lineItemDiscount")) "OrderLineTotalAmountWoVat",
                SUM(("VatBaseAmount"*"Quantity")-("Quantity"*"lineItemDiscount")) "OrderLineTotalAmountVat",
                SUM("VatBaseAmount") "OrderLineTotalVatBaseAmount", 
                SUM("lineItemPrice")-SUM("VatBaseAmount") "OrderLineItemTax",
                "SalesCampaignId","OrderLineItemStatusName"
            FROM stage."OrderLines"
                    GROUP BY Cast("OrderDate" as Date) ,"OrderId", "OrderLineId", "MerchantId", "ProductCode","UnitPrice"
                             ,"CurrencyCode", "SalesCampaignId", "OrderLineItemStatusName";
            
            UPDATE "Fact"."OrderLines" AS ol
            SET
                "CurrencyCode" = sol."CurrencyCode",
                "UnitPrice" = sol."UnitPrice",
                "OrderLineTotalQuantity" = sol."OrderLineTotalQuantity",
                "OrderLineTotalItemPrice" = sol."OrderLineTotalItemPrice",
                "OrderLineTotalItemDiscount" = sol."OrderLineTotalItemDiscount",
                "OrderLineTotalItemTyDiscount" = sol."OrderLineTotalItemTyDiscount",
                "OrderLineTotalAmountWoVat" = sol."OrderLineTotalAmountWoVat",
                "OrderLineTotalAmountVat" = sol."OrderLineTotalAmountVat",
                "OrderLineTotalVatBaseAmount" = sol."OrderLineTotalVatBaseAmount",
                "OrderLineItemTax" = sol."OrderLineItemTax",
                "SalesCampaignId" = sol."SalesCampaignId",
                "OrderLineItemStatusName" = sol."OrderLineItemStatusName"
            FROM 
                stg_OrderLines AS sol
            WHERE 
                ol."OrderId" = sol."OrderId" AND ol."OrderDate" = sol."OrderDate";
            

            INSERT INTO "Fact"."OrderLines"
            SELECT  sol."OrderDate" ,
				    sol."OrderId" ,
				    sol."OrderLineId" ,
				    sol."MerchantId" ,
				    sol."ProductCode" ,
				    sol."CurrencyCode" ,
				    sol."UnitPrice" ,
				    sol."OrderLineTotalQuantity" ,
				    sol."OrderLineTotalItemPrice" ,
				    sol."OrderLineTotalItemDiscount" ,
				    sol."OrderLineTotalItemTyDiscount" ,
				    sol."OrderLineTotalAmountWoVat" ,
				    sol."OrderLineTotalAmountVat" ,
				    sol."OrderLineTotalVatBaseAmount" ,
				    sol."OrderLineItemTax" ,
				    sol."SalesCampaignId" ,
				    sol."OrderLineItemStatusName" 
            FROM stg_OrderLines AS sol
            LEFT JOIN "Fact"."OrderLines" AS ol ON ol."OrderId" = sol."OrderId" AND ol."OrderDate" = sol."OrderDate"
            WHERE ol."OrderId" IS NULL;
        """)

        # Değişiklikleri kaydet ve bağlantıyı kapat
        pg_connection.commit()
        print("FactOrderLines Data transfer completed.")

    except psycopg2.Error as e:
        print("FactOrderLines Error transferring data:", e)

    finally:
        pg_cursor.close()
        pg_connection.close()

# Veri aktarımını başlat
transfer_data()
