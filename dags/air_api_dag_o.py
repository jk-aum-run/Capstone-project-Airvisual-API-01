from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from datetime import datetime  
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests

import great_expectations as gx
import pandas as pd
import requests
from great_expectations.dataset import PandasDataset


def _get_airquality_data(**context):
    # API_KEY = os.environ.get("WEATHER_API_KEY")
    API_KEY = Variable.get("air_api_key")

    name = Variable.get("name")
    print(f"Hello, {name}")

    print(context)
    print(context["execution_date"])
    ds = context["ds"]
    print(ds)


def _get_airquality_data():
    # API_KEY = "9302d96b-c689-4b87-93cc-05e1a48e9aa3"
    API_KEY = Variable.get("air_api_key")
    city = "Chatuchak"
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

# **2️⃣ TRANSFORM: แปลงข้อมูลจาก JSON**
def transform_data():
    try:
        with open("/opt/airflow/dags/airquality.json", "r") as f:
            data = json.load(f)

pollution = data["data"]["current"]["pollution"]
        weather = data["data"]["current"]["weather"]

        transformed_data = {
            "city": data["data"]["city"],
            "state": data["data"]["state"],
            "country": data["data"]["country"],
            "timestamp": pollution["ts"],
            "aqius": pollution["aqius"],
            "mainus": pollution["mainus"],
            "aqicn": pollution["aqicn"],
            "maincn": pollution["maincn"],
            "temp": weather["tp"],
            "pressure": weather["pr"],
            "humidity": weather["hu"],
            "wind_speed": weather["ws"],
            "wind_direction": weather["wd"],
            "weather_icon": weather["ic"]
        }

        with open("/opt/airflow/dags/airquality_transformed.json", "w") as f:
            json.dump(transformed_data, f)
        
        print("✅ Transformed data saved to airquality_transformed.json")
    
    except Exception as e:
        print(f"❌ Error transforming data: {e}")

        

with DAG(
    "air_quality_api_dag",
    schedule_interval='30 14 * * *',  
    start_date=datetime(2025, 1, 1),  
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




