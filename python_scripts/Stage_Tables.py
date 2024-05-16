from sqlalchemy import create_engine
from configparser import ConfigParser
import pandas as pd
import json

config = ConfigParser()
config.read('db_config.ini')

# PostgreSQL bağlantı bilgilerini alıyoruz.
postgresql_environment = 'dbinstance'
postgresql_server = config.get(postgresql_environment,'server')
postgresql_username = config.get(postgresql_environment,'user')
postgresql_password = config.get(postgresql_environment,'password')
postgresql_database= config.get(postgresql_environment,'database')

# PostgreSQL bağlantını oluşturduk.
pg_engine = create_engine(f'postgresql+psycopg2://{postgresql_username}:{postgresql_password}@{postgresql_server}:5432/{postgresql_database}')

# sample_data dosyasını okuyoruz
with open("../Data/sample_data.json", 'r', encoding='utf-8') as file:
    data = json.load(file)

def insert_Order_Collection_postgresql():
    orders_list = []
    orderLines_list = []
    customers_list = []
    orderAddress_list = []
    product_list = []
    shipment_list = []
    orderPackageHistories_list = []

    for order in data:

        order_info = {
            "OrderId": order["id"],
            "OrderNumber": order["orderNumber"],
            "WarehouseId": order["warehouseId"],
            "OrderDate": pd.to_datetime(order["orderDate"], unit="ms"),
            "LastModifiedDate": pd.to_datetime(order["lastModifiedDate"], unit="ms"),
            "OrderStatus": order["status"],
            "DeliveryType": order["deliveryType"],
            "IsCommercial": order["commercial"],
            "CustomerId": order["customerId"],
            "TotalPrice": order["totalPrice"],
            "TotalDiscount": order["totalDiscount"],
            "CurrencyCode": order["currencyCode"],
            "IsFastDelivery": order["fastDelivery"],
            "IsGiftBoxRequest": order["giftBoxRequested"]
        }
        orders_list.append(order_info)


        for line in order["lines"]:
            for discount_detail in line["discountDetails"]:
                line_info = {
                    "OrderId": order["id"],
                    "OrderLineId": line["id"],
                    "MerchantId": line["merchantId"],
                    "OrderDate": pd.to_datetime(order["orderDate"], unit="ms"),
                    "ProductCode": line["productCode"],
                    "Quantity": line["quantity"],
                    "UnitPrice": line["price"],
                    "lineItemPrice": discount_detail["lineItemPrice"],
                    "lineItemDiscount": discount_detail["lineItemDiscount"],
                    "lineItemTyDiscount": discount_detail["lineItemTyDiscount"],
                    "CurrencyCode": line["currencyCode"],
                    "SalesCampaignId": line["salesCampaignId"],
                    "VatBaseAmount": line["vatBaseAmount"],
                    "OrderLineItemStatusName": line["orderLineItemStatusName"],
                }
                orderLines_list.append(line_info)


            product_info = {
                "ProductCode": line["productCode"],
                "ProductSku": line["sku"],
                "ProductBarcode": line["barcode"],
                "Price": line["price"],
                "ProductSize": line["productSize"],
                "ProductColor": line["productColor"],
                "ProductOrigin": line["productOrigin"],
                "Amount": line["amount"]
            }
            product_list.append(product_info)

        customer_info = {
            "CustomerId": order["customerId"],
            "CustomerFirstName": order["customerFirstName"],
            "CustomerLastName": order["customerLastName"],
            "CustomerEmail": order["customerEmail"],
            "PhoneNumber": order["invoiceAddress"]["phone"],
            "TcIdentityNumber": order["tcIdentityNumber"],
            "TaxNumber": order["taxNumber"]
        }
        customers_list.append(customer_info)

        for order_package in order["packageHistories"]:
            order_package_info = {
                "OrderId": order["id"],
                "ProductCode": line.get("productCode"),
                "Status": order_package.get("status"),
                "CreatedDate": pd.to_datetime(order_package.get("createdDate"), unit="ms")
            }
            orderPackageHistories_list.append(order_package_info)

        for address_type in ["invoiceAddress", "shipmentAddress"]:
            address_info = {
                "Id": order[address_type]["id"],
                "OrderId": order["id"],
                "CustomerId": order["customerId"],
                "OrderAddressType": address_type,
                "Company": order[address_type]["company"],
                "Address1": order[address_type]["address1"],
                "Address2": order[address_type]["address2"],
                "CityCode": order[address_type]["cityCode"],
                "City": order[address_type]["city"],
                "DistrictId": order[address_type]["districtId"],
                "District": order[address_type]["district"],
                "PostalCode": order[address_type]["postalCode"],
                "CountryCode": order[address_type]["countryCode"],
                "FullAddress": order[address_type]["fullAddress"],
                "NeighborhoodId": order[address_type]["neighborhoodId"],
                "Neighborhood": order[address_type]["neighborhood"]
            }
            orderAddress_list.append(address_info)

        shipment_info = {
            "ShipmentAddressId": order["shipmentAddress"]["id"],
            "OrderId": order["id"],
            "CustomerId": order["customerId"],
            "CargoProviderName": order["cargoProviderName"],
            "CargoTrackingNumber": order["cargoTrackingNumber"],
            "ShipmentPackageStatus": order["shipmentPackageStatus"],
            "DeliveryAddressType": order["deliveryAddressType"],
            "CargoTrackingLink": order["cargoTrackingLink"],
            "AgreedDeliveryDate": pd.to_datetime(order["agreedDeliveryDate"], unit="ms"),
            "OriginShipmentDate": pd.to_datetime(order["originShipmentDate"], unit="ms"),
            "EstimatedDeliveryStartDate": pd.to_datetime(order["estimatedDeliveryStartDate"], unit="ms"),
            "EstimatedDeliveryEndDate": pd.to_datetime(order["estimatedDeliveryStartDate"], unit="ms"),
            "AgreedDeliveryExtensionStartDate": pd.to_datetime(order["agreedDeliveryExtensionStartDate"], unit="ms"),
            "AgreedDeliveryExtensionEndDate": pd.to_datetime(order["agreedDeliveryExtensionEndDate"], unit="ms")
        }

        shipment_list.append(shipment_info)

    df_Orders = pd.DataFrame(orders_list)
    df_OrderLines = pd.DataFrame(orderLines_list)
    df_Customers = pd.DataFrame(customers_list)
    df_OrderAddress = pd.DataFrame(orderAddress_list)
    df_Product = pd.DataFrame(product_list)
    df_Shipment = pd.DataFrame(shipment_list)
    df_OrderPackageHistories = pd.DataFrame(orderPackageHistories_list)

    # Verileri PostgreSQL tablolarına yazıyoruz
    df_Orders.to_sql('Orders', pg_engine, if_exists='replace', index=False, schema='stage')
    df_OrderLines.to_sql('OrderLines', pg_engine, if_exists='replace', index=False, schema='stage')
    df_Customers.to_sql('Customers', pg_engine, if_exists='replace', index=False, schema='stage')
    df_OrderAddress.to_sql('OrderAddress', pg_engine, if_exists='replace', index=False, schema='stage')
    df_Shipment.to_sql('Shipment', pg_engine, if_exists='replace', index=False, schema='stage')
    df_Product.to_sql('Product', pg_engine, if_exists='replace', index=False, schema='stage')
    df_OrderPackageHistories.to_sql('OrderPackageHistory', pg_engine, if_exists='replace', index=False, schema='stage')

    print("Stage Tables Data transfer completed.")

insert_Order_Collection_postgresql()
