from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'Muhammad Arkan Nibrastama',
}

with DAG(
    dag_id='spark_upsert_to_iceberg',
    default_args=default_args,
    schedule_interval='1 0 * * *',
    start_date = datetime(2024, 1, 1),
    catchup=False,
) as dag:

    upsert_to_arm_robot_iceberg = SparkSubmitOperator(
        task_id='upsert_to_arm_robot_iceberg',
        application='/opt/airflow/jobs/upsert_to_iceberg_arm_robot.py',
        conn_id='spark_default',
        jars='/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/airflow/jars/nessie-spark-extensions-3.5_2.12-0.76.6.jar',
        application_args=['']
    )

    upsert_to_cnc_machine_iceberg = SparkSubmitOperator(
        task_id='upsert_to_cnc_machine_iceberg',
        application='/opt/airflow/jobs/upsert_to_iceberg_cnc_machine.py',
        conn_id='spark_default',
        jars='/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/airflow/jars/nessie-spark-extensions-3.5_2.12-0.76.6.jar',
        application_args=['']
    )

    upsert_to_injection_molding_machine_iceberg = SparkSubmitOperator(
        task_id='upsert_to_injection_molding_machine_iceberg',
        application='/opt/airflow/jobs/upsert_to_iceberg_injection_molding_machine.py',
        conn_id='spark_default',
        jars='/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/airflow/jars/nessie-spark-extensions-3.5_2.12-0.76.6.jar',
        application_args=['']
    )

    upsert_to_battery_assembly_machine_iceberg = SparkSubmitOperator(
        task_id='upsert_to_battery_assembly_machine_iceberg',
        application='/opt/airflow/jobs/upsert_to_iceberg_battery_assembly_machine.py',
        conn_id='spark_default',
        jars='/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/airflow/jars/nessie-spark-extensions-3.5_2.12-0.76.6.jar',
        application_args=['']
    )

    # if you have a lot of resources, you can run them in parallel
    # [upsert_to_arm_robot_iceberg, 
    #  upsert_to_cnc_machine_iceberg, 
    #  upsert_to_injection_molding_machine_iceberg, 
    #  upsert_to_battery_assembly_machine_iceberg]

    # if not, run in sequence so that it doesn't lag
    upsert_to_arm_robot_iceberg >> upsert_to_cnc_machine_iceberg >> upsert_to_injection_molding_machine_iceberg >> upsert_to_battery_assembly_machine_iceberg
