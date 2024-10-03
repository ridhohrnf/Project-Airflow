from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import json
import os
import openpyxl
import io

folder_path = '/opt/airflow/data/cashier_data'
excel_file_path = '/opt/airflow/data/master_store.xlsx'  
base_folder = '/opt/airflow/data/sales_data'
new_base_folder='/opt/airflow/data/new_sales_data'

default_start_date = datetime.now() - timedelta(days=3)
default_end_date = datetime.now() - timedelta(days=1)

var_start_date_str = Variable.get("var_start_date", default_var=None)
var_end_date_str = Variable.get("var_end_date", default_var=None)

if var_start_date_str and var_end_date_str:
    start_date = datetime.strptime(var_start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(var_end_date_str, '%Y-%m-%d')
else:
    start_date = default_start_date
    end_date = default_end_date

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def process_json_to_postgres(**kwargs):
    try:
        print("Current working directory:", os.getcwd())
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        for file_name in os.listdir(folder_path):
            if file_name.endswith('.json'):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r') as file:
                    input_data = json.load(file)
                    modified_data = []

                    for key, value in input_data.items():
                        record = {
                            'CashierId': value.get('CashierId', None),
                            'Name': value.get('Name', None),
                            'Email': value.get('Email', None),
                            'Level': value.get('Level', None),
                            'date': key
                        }
                        modified_data.append(record)

                    for record in modified_data:
                        cashier_id = record['CashierId']
                        name = record['Name']
                        email = record['Email']
                        level = record['Level']
                        date = record['date'][:8]

                        insert_query = """
                            INSERT INTO raw.cashier (cashier_id, name, email, level, date)
                            VALUES (%s, %s, %s, %s, %s::date)
                            ON CONFLICT (cashier_id) DO UPDATE 
                            SET
                                email = EXCLUDED.email,
                                level = EXCLUDED.level,
                                date = EXCLUDED.date::date;
                        """
                        pg_hook.run(insert_query, parameters=(cashier_id, name, email, level, date))
    except Exception as e:
        print(f"Error processing JSON to PostgreSQL: {e}")

def read_xlsx_insert_postgre(**kwargs):
    file_path=kwargs['file_path']
    df=pd.read_excel(file_path)
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    schema_table="raw.reff_store"

    insert_query = f"""
    INSERT INTO {schema_table}
    VALUES (%s, %s, %s)
    ON CONFLICT (store_id) DO nothing;
    ;
    """
    data_store = []
    for index, row in df.iterrows():
        data_store.append((row['StoreID'], row['StoreName'], row['Location'])) 
    cursor.executemany(insert_query, data_store)

    conn.commit()
    cursor.close()
    conn.close()
    print("Data berhasil dimasukkan ke tabel PostgreSQL.")

def get_all_data():
    base_folder='/opt/airflow/data/sales_data'
    all_dfs = []

    for parquet_file in Path(base_folder).glob('Year=*/Month=*/Day=*/*.parquet'):
        year = parquet_file.parts[-4].split('=')[1]
        month = parquet_file.parts[-3].split('=')[1]
        day = parquet_file.parts[-2].split('=')[1]

        date_str = f'{year}-{month.zfill(2)}-{day.zfill(2)}'

        df = pd.read_parquet(parquet_file)
        df['date'] = date_str

        all_dfs.append(df)

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame()

    return final_df

def ingest_to_postgres(**kwargs):
    df = get_all_data()
    
    if not df.empty:
        min_date = df['date'].min()
        max_date=df['date'].max()

        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        delete_query="""
        DELETE FROM raw.sales 
        WHERE date >= %s AND date <= %s
        """
        cursor.execute(delete_query, (min_date, max_date))
        connection.commit()

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        insert_query = """
        COPY raw.sales 
        FROM STDIN WITH CSV
        DELIMITER ',' 
        NULL 'NULL'
        """
        
        cursor.copy_expert(insert_query, csv_buffer)
        connection.commit()

        print(f"Data berhasil di-ingest ke tabel PostgreSQL, jumlah rows: {len(df)} dari tanggal {min_date} sampai {max_date}")


def get_new_data(start_date=None, end_date=None):
    all_df = []
    new_base_folder = '/opt/airflow/data/new_sales_data'

    current_date = start_date
    while current_date <= end_date:
        file_name = f"sales_data_{current_date.strftime('%Y-%m')}_{str(current_date.day).zfill(2)}.csv"
        file_path = Path(new_base_folder) / file_name

        if file_path.exists():
            df = pd.read_csv(file_path)
            print(f"Read {file_path}")
            df['date'] = current_date.strftime('%Y-%m-%d') 
            all_df.append(df)
        else:
            print(f"File not found: {file_path}")

        current_date += timedelta(days=1)

    if all_df:
        final_df = pd.concat(all_df, ignore_index=True)
    else:
        final_df = pd.DataFrame() 

    if not final_df.empty:
        min_date_new = final_df['date'].min()  
        max_date_new = final_df['date'].max()  

        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        try:
            delete_query_new = """
            DELETE FROM raw.sales 
            WHERE date >= %s AND date <= %s
            """
            cursor.execute(delete_query_new, (min_date_new, max_date_new))
            connection.commit()

            csv_buffer = io.StringIO()
            final_df.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)

            insert_query_new = """
            COPY raw.sales 
            FROM STDIN WITH CSV
            DELIMITER ',' 
            NULL 'NULL'
            """
            cursor.copy_expert(insert_query_new, csv_buffer)
            connection.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
            connection.rollback() 
        finally:
            cursor.close() 
            connection.close() 

    return final_df

with DAG(dag_id='ETL_from_datasource_to_raw_to_data_mart', default_args=default_args, schedule_interval='0 7,8 * * *', catchup=False) as dag:

    ingest_reff_cashier = PythonOperator(
        task_id='ingest_reff_cashier',
        python_callable=process_json_to_postgres,
        provide_context=True
    )

    ingest_reff_store = PythonOperator(
        task_id='ingest_reff_store',
        python_callable=read_xlsx_insert_postgre,
        op_kwargs={
            'file_path': excel_file_path,
        }
    )

    ingest_sales_data = PythonOperator(
    task_id='ingest_sales_data',
    python_callable=ingest_to_postgres,
    dag=dag,
    )

    ingest_new_sales_data = PythonOperator( 
    task_id="ingest_new_sales_data",
    python_callable=get_new_data,
    op_kwargs={'start_date': start_date, 'end_date': end_date},
    dag=dag
    )
    data_mart_sales = PostgresOperator(
        task_id='data_mart_sales_task',
        sql='./sql/data_mart.sql',
        postgres_conn_id='postgres_default',  
    )

    ingest_reff_cashier >> data_mart_sales
    ingest_reff_store >>data_mart_sales
    ingest_sales_data >> ingest_new_sales_data >>data_mart_sales