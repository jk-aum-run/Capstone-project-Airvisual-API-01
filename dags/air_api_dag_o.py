from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from datetime import datetime  

import requests

def _get_airquality_data():
    API_KEY = "9302d96b-c689-4b87-93cc-05e1a48e9aa3"
    city = "Bangkok"
    state = "Bangkok"
    country = "Thailand"

    url = f"http://api.airvisual.com/v2/city?city={city}&state={state}&country={country}&key={API_KEY}"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        print("✅ API URL:", response.url)
        print("✅ Data:", data)
    else:
        print(f"❌ Error {response.status_code}: {response.text}")

with DAG(
    "air_quality_api_dag",
    schedule_interval='30 14 * * *',  # ✅ รันทุกวันเวลา 14:30 UTC (21:30 ไทย)
    start_date=datetime(2025, 1, 1),  # ✅ ต้องปิดวงเล็บให้ครบ
    catchup=False,  # ✅ ต้องแยกออกจาก start_date
    tags=["dpu", "air_dag_oum"],
) as dag:  
    start = EmptyOperator(task_id="start")

    get_airquality_data = PythonOperator(
        task_id="get_airquality_data",
        python_callable=_get_airquality_data,
    )

    end = EmptyOperator(task_id="end")

    start >> get_airquality_data >> end
