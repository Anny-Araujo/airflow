from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #importação do módulo

'''
Criando argumentos default para DAGs.
Dessa forma, não precisamos ficar repetindo os argumentos em todas as DAGs. 
'''

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 9),
    'email': ['teste@teste.com'], #pode ser um ou mais e-mails
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}
dag= DAG('dag_with_arguments', description='DAG com argumentos default',
         default_args=default_args,
         schedule_interval='@hourly',
         start_date=datetime(2025, 8, 9),
         catchup=False, default_view='graph', tags=['processo', 'teste', 'pipeline'])

task_1= BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag, retries=3) #a nível da task sobrescrevemos o default_args retries
task_2= BashOperator(task_id="tsk2", bash_command="sleep 10", dag=dag)
task_3= BashOperator(task_id="tsk3", bash_command="sleep 15", dag=dag)

task_1 >> task_2 >> task_3
