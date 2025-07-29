from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag= DAG('trigger_dag_4', description='Dag com mais tasks',
         schedule_interval=None,
         start_date=datetime(2025, 7, 28),
         catchup=False)

'''
Definindo triggers a nível de task
Criamos mais tasks para aumentar a compleidade das precedencia e testar o trigger rule de one_failed
Se, pelo menos, uma task falhar a task 9 será executada.
'''

task_1= BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task_2= BashOperator(task_id="tsk2",bash_command="sleep 5", dag=dag)
task_3= BashOperator(task_id="tsk3",bash_command="sleep 5", dag=dag)
task_4= BashOperator(task_id="tsk4",bash_command="sleep 5", dag=dag)
task_5= BashOperator(task_id="tsk5",bash_command="sleep 5", dag=dag)
task_6= BashOperator(task_id="tsk6",bash_command="sleep 5", dag=dag)
task_7= BashOperator(task_id="tsk7",bash_command="sleep 5", dag=dag)
task_8= BashOperator(task_id="tsk8",bash_command="sleep 5", dag=dag)
task_9= BashOperator(task_id="tsk9",bash_command="sleep 5", dag=dag,
                     trigger_rule='one_failed')

task_1 >> task_2 #task 1 chama task 2
task_3 >> task_4 # task chama task 4
[task_2, task_4] >> task_5 >> task_6 # task 2 e 4 chama as tasks 5 e 6
task_6 >> [task_7, task_8, task_9] # task 6 chama as tasks 7, 8 e 9