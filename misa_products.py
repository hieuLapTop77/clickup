import os
import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago
import glob
from common.utils import download_file_drive
# Variables

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
    schedule_interval="30 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["Misa", "product", "san pham"],
    max_active_runs=1,
)
def Misa_products():
    @task
    def remove_files():
        files = glob.glob(os.path.join(TEMP_PATH, '*'))
        for i in files:
            try:
                os.remove(i)
                print(f"Deleted file: {i}")
            except Exception as e:
                print(f"Error deleting file {i} : {e}")
    ######################################### API ################################################
    @task
    def download_latest_file() -> str:
        folder_name = 'sanpham'
        folder_id = Variable.get("folder_id_sanpham")
        return download_file_drive(folder_name=folder_name, folder_id=folder_id)

    ######################################### INSERT DATA ################################################
    @task
    def insert_products(file_local: str) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        # sql_del = "delete from [dbo].[3rd_misa_sales_details_v1] where ;"
        # cursor.execute(sql_del)

        df = pd.read_excel(file_local, skiprows=2, index_col=None, engine='openpyxl', skipfooter=1, header=[0,1])
        sql_del = f"delete from [dbo].[3rd_misa_products] where [Mã] in {tuple(df['Mã']['Unnamed: 1_level_1'].tolist())};"
        print(sql_del)
        cursor.execute(sql_del)
        values = []
        if len(df) > 0:
 
            sql = """
                    INSERT INTO [dbo].[3rd_misa_products](
                        [STT]
                        ,[Mã]
                        ,[Tên]
                        ,[Giảm thuế theo quy định]
                        ,[Tính chất]
                        ,[Nhóm VTHH]
                        ,[Đơn vị tính chính]
                        ,[Số lượng tồn]
                        ,[Giá trị tồn]
                        ,[Thời hạn bảo hành]
                        ,[Số lượng tồn tối thiểu]
                        ,[Nguồn gốc]
                        ,[Mô tả]
                        ,[Diễn giải khi mua]
                        ,[Diễn giải khi bán]
                        ,[Mã kho ngầm định]
                        ,[Kho ngầm định]
                        ,[TK Kho]
                        ,[TK Doanh thu]
                        ,[TK chiết khấu]
                        ,[TK giảm giá]
                        ,[TK Trả lại]
                        ,[TK chi phí]
                        ,[Tỷ lệ CK khi mua hàng]
                        ,[Đơn giá mua cố định]
                        ,[Đơn giá mua gần nhất]
                        ,[Đơn giá bán 1]
                        ,[Đơn giá bán 2]
                        ,[Đơn giá bán 3]
                        ,[Đơn giá bán cố định]
                        ,[Là đơn giá sau thuế]
                        ,[Thuế suất GTGT]
                        ,[% thuế suất KHAC]
                        ,[Thuế suất thuế NK]
                        ,[Thuế suất thuế XK]
                        ,[Nhóm HHDV chịu thuế TTĐB]
                        ,[Trạng thái]
                        ,[Chiết khấu_Số lượng từ]
                        ,[Chiết khấu_Số lượng đến]
                        ,[Chiết khấu_%_chiết khấu]
                        ,[Chiết khấu_Số tiền chiết khấu]
                        ,[Đơn vị chuyển đổi_Đơn vị chuyển đổi]
                        ,[Đơn vị chuyển đổi_Tỷ lệ chuyển đổi]
                        ,[Đơn vị chuyển đổi_Phép tính]
                        ,[Đơn vị chuyển đổi_Mô tả]
                        ,[Đơn vị chuyển đổi_Đơn giá bán 1]
                        ,[Đơn vị chuyển đổi_Đơn giá bán 2]
                        ,[Đơn vị chuyển đổi_Đơn giá bán 3]
                        ,[Đơn vị chuyển đổi_Đơn giá cố định]
                        ,[Định mức nguyên vật liệu_Mã nguyên vật liệu]
                        ,[Định mức nguyên vật liệu_Tên nguyên vật liệu]
                        ,[Định mức nguyên vật liệu_Đơn vị tính]
                        ,[Định mức nguyên vật liệu_Số lượng]
                        ,[Định mức nguyên vật liệu_Khoản mục CP]
                        ,[Mã quy cách_Tên quy cách]
                        ,[Mã quy cách_Cho phép trùng]
                        ,[dtm_creation_date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, getdate())
                """
            for _index, row in df.iterrows():
                value = (
                            str(row[0]),
                            str(row[1]),
                            str(row[2]),
                            str(row[3]),
                            str(row[4]),
                            str(row[5]), 
                            str(row[6]),
                            str(row[7]),
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
                            str(row[24]),
                            str(row[25]),
                            str(row[26]),
                            str(row[27]),
                            str(row[28]),
                            str(row[29]),
                            str(row[30]),
                            str(row[31]),
                            str(row[32]),
                            str(row[33]),
                            str(row[34]),
                            str(row[35]), 
                            str(row[36]),
                            str(row[37]),
                            str(row[38]),
                            str(row[39]),
                            str(row[40]),
                            str(row[41]),
                            str(row[42]),
                            str(row[43]),
                            str(row[44]),
                            str(row[45]), 
                            str(row[46]),
                            str(row[47]),
                            str(row[48]),
                            str(row[49]),
                            str(row[50]),
                            str(row[51]),
                            str(row[52]),
                            str(row[53]),
                            str(row[54]),
                            str(row[55]), 
                )
                values.append(value)
            cursor.executemany(sql, values)

        print(f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############

    local_file = download_latest_file()
    insert_task = insert_products(local_file)
    remove_files() >> local_file >> insert_task


dag = Misa_products()
