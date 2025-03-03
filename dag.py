from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import pendulum

local_tz = pendulum.timezone("Asia/Seoul")

# Extract 단계
def extract() -> dict:
    data = pd.read_csv('/opt/airflow/dags/airflow-dag-k8s/pokemon_data_pokeapi.csv')

    return data.to_dict(orient="records")

# Transform 단계
def transform(ti: dict) -> dict:
    data = ti.xcom_pull(task_ids="extract")
    df = pd.DataFrame(data)
    
    legendary_data = df[df['Legendary Status'] == 'Yes']

    # NaN 값 변환
    legendary_data = legendary_data.fillna("NULL")
    
    # Abilities를 문자열로 변환
    legendary_data["Abilities"] = legendary_data["Abilities"].apply(lambda x: x.replace(",", "|"))

    return legendary_data.to_dict(orient='records')

# Load 단계 (MySQL에 저장)
def load(ti : dict) -> None:
    data = ti.xcom_pull(task_ids="transform")
    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    # 테이블이 없으면 생성하는 SQL
    create_table_query = """
    CREATE TABLE IF NOT EXISTS pokemon_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100),
        pokedex_number INT,
        type1 VARCHAR(50),
        type2 VARCHAR(50) NULL,
        classification VARCHAR(100),
        height FLOAT,
        weight FLOAT,
        abilities TEXT,
        generation INT,
        legendary_status VARCHAR(10)
    );
    """
    cursor.execute(create_table_query)

    # 데이터 삽입 SQL
    insert_query = """
    INSERT INTO pokemon_data 
    (name, pokedex_number, type1, type2, classification, height, weight, abilities, generation, legendary_status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for row in data:
        cursor.execute(insert_query, (
            row["Name"], row["Pokedex Number"], row["Type1"], row["Type2"],
            row["Classification"], row["Height (m)"], row["Weight (kg)"],
            row["Abilities"], row["Generation"], row["Legendary Status"]
        ))

    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner' : 'admin',
    'depends_on_past' : False,
    'start_date' : datetime(2025,2,28, tzinfo=local_tz),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    "pokemon_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for Pokémon data",
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load
    )

    extract_task >> transform_task >> load_task