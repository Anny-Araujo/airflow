from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
'''
Esta DAG demonstra como utilizar o provider Postgres no Airflow para orquestrar operações em um banco de dados PostgreSQL.

- O provider Postgres adiciona operadores específicos, como o `PostgresOperator`, que permitem executar comandos SQL diretamente 
em bancos PostgreSQL.
- O parâmetro `postgres_conn_id` deve ser previamente configurado na interface do Airflow, apontando para a conexão do banco de 
dados. Configure-o em Admin > Connections, criando uma conexão com o ID `postgres` e preenchendo os detalhes do banco.
- O fluxo da DAG inclui:
    1. Criação de uma tabela (`create_table`).
    2. Inserção de dados na tabela (`insert_data`).
    3. Consulta dos dados inseridos (`query_data`).
    4. Impressão dos resultados da consulta usando o `PythonOperator` e XCom (`result`).
- O uso de XCom permite compartilhar o resultado da consulta entre tasks, tornando possível processar ou exibir dados após a 
execução de comandos SQL.

Providers no Airflow são pacotes que facilitam a integração com serviços externos, como bancos de dados, APIs, sistemas de 
arquivos e nuvem. Eles trazem operadores, sensores e hooks específicos para cada tecnologia, tornando a automação e orquestração 
de pipelines mais simples e eficiente.
'''

dag= DAG('database', description='Explorando o uso do provider Postgres no Airflow',
         schedule_interval=None,
         start_date=datetime(2025, 8, 10),
         catchup=False)

def result_print(**context):
    query_result = context['task_instance'].xcom_pull(task_ids='query_data')
    print(f"Resultado da query: {query_result}")
    for row in query_result:
        print(f"Row: {row}")

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres',
    sql='create table if not exists teste_table(id int);',
    dag=dag
)

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres',
    sql='insert into teste_table values(1);',
    dag=dag
)

query_data = PostgresOperator(
    task_id='query_data',
    postgres_conn_id='postgres',
    sql='select * from teste_table;',
    dag=dag
)

result = PythonOperator(
    task_id='result',
    python_callable=result_print,
    provide_context=True,
    dag=dag
)

create_table >> insert_data >> query_data >> result