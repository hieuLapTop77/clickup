import json

import airflow.providers.microsoft.mssql.hooks.mssql as mssql
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import task
from airflow.utils.dates import days_ago

# Variables
# Jotform
# JOTFORM_GET_USER = Variable.get("jotform_get_user")
# JOTFORM_GET_USER_USAGE = Variable.get("jotform_get_user_usage")
# JOTFORM_GET_USER_FOLDERS = Variable.get("jotform_get_user_folders")
# JOTFORM_GET_USER_REPORTS = Variable.get("jotform_get_user_reports")

JOTFORM_GET_USER_FORMS = Variable.get("jotform_get_user_forms")
JOTFORM_GET_USER_SUBMISSIONS = Variable.get("jotform_get_user_submissions")
JOTFORM_GET_FORM_BY_ID_QUESTIONS = Variable.get("jotform_get_form_by_id_questions")

# JOTFORM_GET_FORM_BY_ID = Variable.get("jotform_get_form_by_id")
# JOTFORM_GET_FORM_BY_ID_QUESTIONS_BY_QID = Variable.get("jotform_get_form_by_id_questions_by_qid")
# JOTFORM_GET_FORM_PROPERTIES_BY_ID = Variable.get("jotform_get_form_properties_by_id")
# JOTFORM_GET_FORM_PROPERTIES_BY_ID_BY_KEY = Variable.get("jotform_get_form_properties_by_id_by_key")
# JOTFORM_GET_FORM_REPORTS_BY_ID = Variable.get("jotform_get_form_reports_by_id")
# JOTFORM_GET_FORM_FILES_BY_ID = Variable.get("jotform_get_form_files_by_id")
# JOTFORM_GET_FORM_WEBHOOKS_BY_ID = Variable.get("jotform_get_form_webhooks_by_id")
# JOTFORM_GET_FORM_SUBMISSIONS_BY_ID = Variable.get("jotform_get_form_submissions_by_id")
# JOTFORM_GET_FORM_SUBMISSIONS_BY_SUBID = Variable.get("jotform_get_form_submissions_by_subid")
# JOTFORM_GET_REPORT_BY_ID = Variable.get("jotform_get_report_by_id")
# JOTFORM_GET_FOLDER_BY_ID = Variable.get("jotform_get_folder_by_id")

JOTFORM_API_KEY = Variable.get("api_key_jotform")

# Local path
TEMP_PATH = Variable.get("temp_path")

# Token
BEARER_TOKEN = Variable.get("bearer_token")

# Conection
HOOK_MSSQL = Variable.get("mssql_connection")

data = []

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
    tags=["Jotform"],
    max_active_runs=1,
)
def Jotform():

    @task
    def call_api_get_user_forms():
        params = {
            "apiKey": JOTFORM_API_KEY,
            "limit": 1000
        }
        forms = None
        response = requests.get(JOTFORM_GET_USER_FORMS,
                                params=params, timeout=None)
        if response.status_code == 200:
            forms = response.json()
        else:
            print("Error please check api")
        return forms

    @task
    def call_api_get_user_submissions() -> list:
        params = {
            "apiKey": JOTFORM_API_KEY,
            "limit": 1000
        }
        submissions = None
        response = requests.get(
            JOTFORM_GET_USER_SUBMISSIONS, params=params, timeout=None
        )
        print(response.json())
        if response.status_code == 200:
            submissions = response.json()
        else:
            print("Error please check api")
        return submissions

    @task
    def insert_forms(forms) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        sql_del = "delete from [dbo].[3rd_jotform_user_forms];"
        cursor.execute(sql_del)
        if forms.get('content') is not None and (isinstance(forms.get('content'), list)):
            df = pd.DataFrame(forms.get('content'))
            sql = """
                    INSERT INTO [dbo].[3rd_jotform_user_forms](
                        [form_id]
                        ,[username]
                        ,[title]
                        ,[height]
                        ,[status]
                        ,[created_at]
                        ,[updated_at]
                        ,[last_submission]
                        ,[new]
                        ,[count]
                        ,[type]
                        ,[favorite]
                        ,[archived]
                        ,[url]
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
                """
            values = []
            for _index, row in df.iterrows():
                value = (str(row[0]),
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
                         str(row[13]))
                values.append(value)
            cursor.executemany(sql, values)
            print(
                f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
            sql_conn.commit()
            sql_conn.close()
        else:
            print("Response not correct format: ", forms)

    @task
    def insert_form_submissions(submissions) -> None:
        hook = mssql.MsSqlHook(HOOK_MSSQL)
        sql_conn = hook.get_conn()
        cursor = sql_conn.cursor()
        if submissions.get('content') is not None and (isinstance(submissions.get('content'), list)):
            df = pd.DataFrame(submissions.get('content'))
            sql_del = f"delete from [dbo].[3rd_fillout_form_metadata] where id in {tuple(df['id'].tolist())};"
            cursor.execute(sql_del)
            sql_conn.commit()
            values = []
            df["answers"] = df["answers"].apply(lambda x: json.dumps(x))
            sql = """
                    INSERT INTO [dbo].[3rd_jotform_form_submissions](
                        [id]
                        ,[form_id]
                        ,[ip]
                        ,[created_at]
                        ,[status]
                        ,[new]
                        ,[flag]
                        ,[notes]
                        ,[updated_at]
                        ,[answers]
                        ,[dtm_Creation_Date])
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, getdate())
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

                )
                values.append(value)
            cursor.executemany(sql, values)
        print(
            f"Inserted {len(values)} rows in database with {df.shape[0]} rows")
        sql_conn.commit()
        sql_conn.close()


    ############ DAG FLOW ############
    forms = call_api_get_user_forms()
    insert_forms_task = insert_forms(forms)
    submissions = call_api_get_user_submissions()
    insert_form_metadata_task = insert_form_submissions(submissions)
    insert_forms_task >> submissions >> insert_form_metadata_task 


dag = Jotform()
