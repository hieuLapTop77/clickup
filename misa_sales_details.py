import json
import os
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago

# Variables
## MISA
API_KEY = Variable.get("api_key")
FOLDER_ID = Variable.get("folder_id")
URL_DRIVER_REQUESTS = Variable.get("url_driver_requests")
URL_DRIVER_DOWNLOAD = Variable.get("url_driver_download")

## Local path
TEMP_PATH = Variable.get("temp_path")

# Khong co TKNNo, TKCo, TKChietKhau, TKGiaVon, TKKho
## Conection
HOOK_MSSQL = Variable.get("mssql_connection")


default_args = {
    "owner": "hieulc",
    "email": ["lechihieu14022000@gmail.com"],
    "email_on_failure": False,
}


@dag(
    default_args=default_args,
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Clickup"],
    max_active_runs=1,
)
def Misa_sales_details():
    ######################################### API ################################################
    @task
    def download_latest_file() -> str:
        url = URL_DRIVER_REQUESTS.format(FOLDER_ID, API_KEY)
        response = requests.get(url)

        response.raise_for_status()
        
        files = response.json().get('files', [])
        
        if not files:
            print('No files found.')
            return
        latest_file = files[0]
        file_id = latest_file['id']
        file_name = latest_file['name']
        
        # URL để tải xuống tệp
        download_url = URL_DRIVER_DOWNLOAD.format(file_id, API_KEY)
        download_response = requests.get(download_url)
        download_response.raise_for_status()
        file_local = TEMP_PATH + file_name
        with open(file_local, 'wb') as f:
            f.write(download_response.content)
        print(f"Downloaded {file_name}")
        return file_local

    ######################################### INSERT DATA ################################################
    @task
    def insert_sales_details(file_local: str) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        # sql_del = "delete from [dbo].[3rd_misa_sales_details_v1] where ;"
        # cursor.execute(sql_del)

        df = pd.read_excel(file_local, skiprows=3, index_col=None, engine='openpyxl')
        sql_del = f"delete from [dbo].[3rd_misa_sales_details_v1] where NgayChungTu >= '{df['Ngày chứng từ'].min()}' and NgayChungTu <= '{df['Ngày chứng từ'].max()}';"
        print(sql_del)
        cursor.execute(sql_del)
        values = []
        if len(df) > 0:
 
            sql = """
                    INSERT INTO [dbo].[3rd_misa_sales_details_v1](
                        [NgayHachToan]
                        ,[NgayChungTu]
                        ,[SoChungTu]
                        ,[DienGiaiChung]
                        ,[customers_code]
                        ,[MaHang]
                        ,[DVT]
                        ,[SoLuongBan]
                        ,[SLBanKhuyenMai]
                        ,[TongSoLuongBan]
                        ,[DonGia]
                        ,[DoanhSoBan]
                        ,[ChietKhau]
                        ,[TongSoLuongTraLai]
                        ,[GiaTriTraLai]
                        ,[ThueGTGT]
                        ,[TongThanhToanNT]
                        ,[GiaVon]
                        ,[MaNhanVienBanHang]
                        ,[TenNhanVienBanHang]
                        ,[MaKho]
                        ,[TenKho]
                        ,[dtm_create_time])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
                """
            for _index, row in df.iterrows():
                value = (
                            str(row[0]),
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            str(row[4]),
                            # str(row[5]), # tên khách
                            str(row[6]),
                            # str(row[7]), # tên hàng
                            str(row[8]),
                            str(row[9]),
                            str(row[10]),
                            str(row[11]),
                            str(row[12]),
                            str(row[13]),
                            str(row[14]),
                            str(row[15]),
                            str(row[16]),
                            str(row[17]),
                            str(row[18]),
                            str(row[19]),
                            str(row[20]),
                            str(row[21]),
                            str(row[22]),
                            str(row[23]),
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############

    local_file = download_latest_file()
    insert_sales_details(local_file)


dag = Misa_sales_details()
