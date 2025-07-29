from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag= DAG('dag_run_dag_2', description='DAG que será invocada por outra DAG',
         schedule_interval=None,
         start_date=datetime(2025, 7, 28),
         catchup=False)

task_1= BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task_2= BashOperator(task_id="tsk2",bash_command="sleep 5", dag=dag)

task_1 >> task_2
