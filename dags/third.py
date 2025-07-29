from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

'''
Estrutura da DAG, cada DAG deve possuir um nome único
'''
dag= DAG('terceira_dag', description='Precedência com as tasks',
         schedule_interval=None,
         start_date=datetime(2025, 7, 28),
         catchup=False)

'''
tasks
O task_id é o nome da sua tarefa, que aparecerá no fluxo
'''
task_1= BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task_2= BashOperator(task_id="tsk2",bash_command="sleep 5", dag=dag)
task_3= BashOperator(task_id="tsk3",bash_command="sleep 5", dag=dag)

'''
Ordem de execução das tasks
Incluir as demais tasks dentro de colchetes para criar um fluxo com tarefas precedentes
'''
[task_1, task_2] >> task_3