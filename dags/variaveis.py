from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable

'''
Resgatando variaveis do Airflow usando o módulo Variable.
Crie uma variável no Airflow, na seção Admin > Variables, com o nome "minha_var" e um valor qualquer.
Use a task abaixo para ler e imprimir o valor da variável.
'''

dag= DAG('variaveis', description='Criando e lendo variáveis no Airflow',
         schedule_interval=None,
         start_date=datetime(2025, 8, 9),
         catchup=False)

def read_variable(**context):
    my_var = Variable.get("minha_var") #aqui você coloca o nome da variável que quer ler
    print(f"O valor da variável é: {my_var}")

task_1= PythonOperator(task_id="tsk1", python_callable=read_variable, dag=dag)

task_1