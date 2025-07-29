from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag= DAG('trigger_dag', description='Definindo triggers rules',
         schedule_interval=None,
         start_date=datetime(2025, 7, 28),
         catchup=False)

'''
Definindo triggers a nível de task
Testando com o trigger one_failed para que pelo menos uma task tenha uma falha e a terceira task seja executada somente
com essa condição.
'''

task_1= BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task_2= BashOperator(task_id="tsk2",bash_command="sleep 5", dag=dag)
task_3= BashOperator(task_id="tsk3",bash_command="sleep 5", dag=dag,
                     trigger_rule='one_failed')

[task_1, task_2] >> task_3