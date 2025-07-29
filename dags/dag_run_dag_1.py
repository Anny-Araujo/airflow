from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #importação do módulo

'''
Criando DAG que chama outra DAG.
Importar o módulo TriggerDagRunOperator e setar ele na task.
Dentro das tasks, devemos passar o argumento de trigger_dag_id que vai receber o id da DAG a ser chamada.
A DAG a ser chamada não pode estar pausada, ela precisa estar ativa. Do contrário, ela entrará na fila e só irá executar
as tasks quando for despausada. 
'''

dag= DAG('dag_run_dag_1', description='DAG que chamará a DAG 2',
         schedule_interval=None,
         start_date=datetime(2025, 7, 28),
         catchup=False)

task_1= BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task_2= TriggerDagRunOperator(task_id="tsk2",trigger_dag_id="dag_run_dag_2", dag=dag)

task_1 >> task_2
