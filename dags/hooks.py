from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
'''
Esta DAG exemplifica o uso do PostgresHook no Airflow para executar operações em um banco de dados PostgreSQL de forma 
programática e flexível.

- O PostgresHook é utilizado para conectar, criar tabelas, inserir dados e consultar registros diretamente via funções Python.
- O parâmetro `postgres_conn_id` deve ser configurado na interface do Airflow, apontando para a conexão do banco.
- O fluxo da DAG segue quatro etapas:
    1. Criação da tabela no banco.
    2. Inserção de dados na tabela.
    3. Consulta dos dados e envio do resultado via XCom.
    4. Impressão dos resultados da consulta.

Cada etapa é implementada como uma função Python e executada por um PythonOperator, mostrando como hooks podem ser usados para 
manipular dados e compartilhar resultados entre tasks.

Hooks são componentes que encapsulam a lógica de conexão e interação com sistemas externos, enquanto providers são pacotes que 
trazem operadores, sensores e hooks para integrações específicas. O uso de hooks permite automações avançadas e integrações 
customizadas em pipelines de dados.
'''

dag= DAG('hook', description='Explorando o uso do hooks no Airflow',
         schedule_interval=None,
         start_date=datetime(2025, 8, 10),
         catchup=False)

def create_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run('CREATE TABLE IF NOT EXISTS hook_table (id int);', autocommit=True) 

def insert_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run('INSERT INTO hook_table VALUES (1);', autocommit=True)   

def select_data(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    result = pg_hook.get_records('SELECT * FROM hook_table;')
    context['task_instance'].xcom_push(key='query_result', value=result)

def print_data(**context):
    query_result = context['task_instance'].xcom_pull(task_ids='select_data_task', key='query_result')
    print(f"Resultado da consulta: {query_result}")
    for row in query_result:
        print(f"Row: {row}")

create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    dag=dag)

insert_data_task = PythonOperator(
    task_id='insert_data_task',
    python_callable=insert_data,
    dag=dag)

select_data_task = PythonOperator(
    task_id='select_data_task',
    python_callable=select_data,
    provide_context=True,
    dag=dag)

print_data_task = PythonOperator(
    task_id='print_data_task',
    python_callable=print_data,
    provide_context=True,
    dag=dag)

create_table_task >> insert_data_task >> select_data_task >> print_data_task