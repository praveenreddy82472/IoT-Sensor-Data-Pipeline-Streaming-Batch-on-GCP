from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta

# Constants / Configuration
PROJECT_ID = 'iot-sensor-462716'
REGION = 'us-west1'  # Ensure this matches your template's region
GCS_TEMPLATE_PATH = 'gs://iot-sensor18/templates/iot_template'
BQ_RAW_TABLE = 'iot-sensor-462716:iot_dataset.iot_raw_data'
BQ_AGG_TABLE = 'iot-sensor-462716:iot_dataset.iot_aggregated_data'
SUBSCRIPTION = 'projects/iot-sensor-462716/subscriptions/iot_sensor.subscription-13555890757533395518'


# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

# Define DAG
with DAG(
    dag_id='iot_data_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description='IoT sensor pipeline: Pub/Sub, Dataflow, BigQuery ML, Reporting'
) as dag:

    run_publisher = BashOperator(
        task_id='run_publisher',
        bash_command='python3 /home/airflow/gcs/data/iot_publisher.py',
        execution_timeout=timedelta(minutes=5),
    )

    run_dataflow = DataflowTemplatedJobStartOperator(
        task_id='run_iot_dataflow_job',
        template=GCS_TEMPLATE_PATH,
        project_id=PROJECT_ID,
        location=REGION,
        job_name='iot-df-{{ ds_nodash }}',
        parameters={
            'input': 'gs://iot-sensor18/iot_batch_data.csv',
            'inputSubscription': SUBSCRIPTION,
            'output_raw': BQ_RAW_TABLE,
            'output_agg': BQ_AGG_TABLE
        },
        wait_until_finished=False,  # Don't wait for streaming job to finish
    )

    wait_5_mins = BashOperator(
        task_id='wait_for_stream',
        bash_command='sleep 600'  # Wait 10 minutes to accumulate data
    )

    bqml_training = BigQueryInsertJobOperator(
        task_id='bqml_training',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE MODEL `iot_dataset.sensor_temp_model_AUTO`
                OPTIONS(model_type='linear_reg', input_label_cols=['temperature']) AS
                SELECT
                  temperature,
                  humidity,
                  EXTRACT(MONTH FROM timestamp) AS month,
                  EXTRACT(DAY FROM timestamp) AS day,
                  EXTRACT(HOUR FROM timestamp) AS hour
                FROM
                  `iot_dataset.iot_raw_data`
                """,
                "useLegacySql": False,
            }
        }
    )

    bqml_predict = BigQueryInsertJobOperator(
        task_id='bqml_predict',
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE VIEW `iot_dataset.predicted_temperature_next_month_auto` AS
                SELECT
                  location,
                  EXTRACT(YEAR FROM timestamp) + IF(EXTRACT(MONTH FROM timestamp) = 12, 1, 0) AS year,
                  MOD(EXTRACT(MONTH FROM timestamp), 12) + 1 AS month,
                  predicted_temperature
                FROM ML.PREDICT(
                  MODEL `iot_dataset.sensor_temp_model_AUTO`,
                  (
                    SELECT
                      temperature,
                      humidity,
                      EXTRACT(MONTH FROM timestamp) AS month,
                      EXTRACT(DAY FROM timestamp) AS day,
                      EXTRACT(HOUR FROM timestamp) AS hour,
                      location,
                      timestamp
                    FROM `iot_dataset.iot_raw_data`
                  )
                )
                """,
                "useLegacySql": False,
            }
        }
    )

    final_step = BashOperator(
        task_id='reporting_done',
        bash_command='echo "Pipeline, predictions and visualization completed."'
    )

    # DAG flow - run publisher and dataflow in parallel, then wait, then ML steps
    [run_publisher, run_dataflow] >> wait_5_mins >> bqml_training >> bqml_predict >> final_step
