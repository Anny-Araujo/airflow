from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import random

'''
Esta DAG exemplifica o conceito de branches no Airflow, permitindo que o fluxo de execução siga caminhos diferentes com 
base em condições dinâmicas.

- O operador BranchPythonOperator é utilizado para decidir, em tempo de execução, qual task será executada a seguir, de 
acordo com o resultado de uma função Python.
- Neste exemplo, uma task gera um número aleatório e, em seguida, a branch decide se o número é par ou ímpar.
- Se o número for par, a DAG segue para a task `task_par`; se for ímpar, segue para a task `task_impar`.
- O uso de branches é útil para criar fluxos condicionais, onde diferentes etapas do pipeline são executadas conforme regras de 
negócio ou resultados intermediários.

Este padrão é ideal para cenários em que decisões precisam ser tomadas durante o processamento, tornando o pipeline mais flexível 
e inteligente.
'''

dag= DAG('dag_branchs', description='DAG Branchs',
         schedule_interval=None,
         start_date=datetime(2025, 8, 9),
         catchup=False)

def generate_random_number():
    return random.randint(1, 100)

task_generate_random_number = PythonOperator(
    task_id='task_generate_random_number', 
    python_callable=generate_random_number,
    dag=dag)

def validate_number(**context):
    number = context['ti'].xcom_pull(task_ids='task_generate_random_number')
    if number % 2 == 0:
        return 'task_par'
    else:
        return 'task_impar'

task_branch = BranchPythonOperator(
    task_id='task_branch',
    python_callable=validate_number,
    provide_context=True,
    dag=dag)


task_par= BashOperator(task_id="task_par", bash_command='echo "Número par"', dag=dag)
task_impar= BashOperator(task_id="task_impar", bash_command='echo "Número impar"', dag=dag)

task_generate_random_number >> task_branch
task_branch >> task_par
task_branch >> task_impar