from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup #fazendo o import do task group

dag= DAG('dag_task_group', description='Criando tasks groups',
         schedule_interval=None,
         start_date=datetime(2025, 7, 28),
         catchup=False)

'''
Criando tasks com task_group.
Definir uma variavel com o objeto TaskGroup, informar um nome único, informar a DAG e quais tasks farão parte desse grupo.
Dentro das tasks, devemos passar o argumento de task_group com o nome da variavel que setamos o grupo.
'''

task_1= BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task_2= BashOperator(task_id="tsk2",bash_command="sleep 5", dag=dag)
task_3= BashOperator(task_id="tsk3",bash_command="sleep 5", dag=dag)
task_4= BashOperator(task_id="tsk4",bash_command="sleep 5", dag=dag)
task_5= BashOperator(task_id="tsk5",bash_command="sleep 5", dag=dag)
task_6= BashOperator(task_id="tsk6",bash_command="sleep 5", dag=dag)

task_group= TaskGroup("tsk_group", dag=dag)

task_7= BashOperator(task_id="tsk7",bash_command="sleep 5", dag=dag, task_group=task_group)
task_8= BashOperator(task_id="tsk8",bash_command="sleep 5", dag=dag, task_group=task_group)
task_9= BashOperator(task_id="tsk9",bash_command="sleep 5", dag=dag, task_group=task_group,
                     trigger_rule='one_failed')

task_1 >> task_2 #task 1 chama task 2
task_3 >> task_4 # task chama task 4
[task_2, task_4] >> task_5 >> task_6 # task 2 e 4 chama as tasks 5 e 6
task_6 >> task_group # task 6 chama o grupo, que contém as tasks 7, 8 e 9