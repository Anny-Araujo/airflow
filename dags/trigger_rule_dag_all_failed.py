from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag= DAG('trigger_dag_3', description='Fazer a task falhar',
         schedule_interval=None,
         start_date=datetime(2025, 7, 28),
         catchup=False)

'''
Definindo triggers a nível de task
Testando com o trigger all_failed para que todas as task anteriores falhem e a terceira task seja executada somente
com essa condição.
O comando exit 1 vai fazer as tasks falharem e fazer com que a task 3 seja executada com sucesso, atendendo a condição
de all_failed.
'''

task_1= BashOperator(task_id="tsk1", bash_command="exit 1", dag=dag)
task_2= BashOperator(task_id="tsk2",bash_command="exit 1", dag=dag)
task_3= BashOperator(task_id="tsk3",bash_command="sleep 5", dag=dag,
                     trigger_rule='all_failed')

[task_1, task_2] >> task_3