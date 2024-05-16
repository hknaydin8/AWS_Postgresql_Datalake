from python_scripts import Stage_Tables, DimCustomers, DimOrderAddress, DimOrderPackageHistory, DimOrders, DimProduct, DimShipments, FactOrderLines

def stage_data_transfer():
    """
    Postgresql'deki stage şemalarına verilerin transferini gerçekleştirir.
    """
    return Stage_Tables.insert_Order_Collection_postgresql()

def transfer_data_to_datalakehouse():
    """
    Verilerin DataLakeHouse ortamına transferini gerçekleştirir.
    """
    DimCustomers.transfer_data()
    DimOrderAddress.transfer_data()
    DimOrderPackageHistory.transfer_data()
    DimOrders.transfer_data()
    DimProduct.transfer_data()
    DimShipments.transfer_data()
    FactOrderLines.transfer_data()

def run_etl_pipeline():
    """
    ETL Pipeline'ını çalıştırır.
    """
    stage_insertion_completed = stage_data_transfer()

    if stage_insertion_completed:
        transfer_data_to_datalakehouse()
    else:
        print("Stage table insertion process is not completed yet. Skipping further steps.")

if __name__ == "__main__":
    run_etl_pipeline()
