from airflow import DAG
from datetime import datetime
from big_data_operator import BigDataOperator #import do operador customizado em plugins/big_data_operator.py
'''
Esta DAG demonstra o uso de um operador customizado (BigDataOperator) criado via plugin no Airflow para manipulação de grandes 
volumes de dados.

- O operador BigDataOperator, definido em `plugins/big_data_operator.py`, permite converter arquivos CSV em formatos Parquet ou 
JSON de forma automatizada.
- As tasks `big_data` e `big_data_task` utilizam o operador para ler um arquivo CSV e salvar o resultado nos formatos Parquet e 
JSON, respectivamente.
- Os parâmetros do operador incluem: caminho do arquivo de entrada (`path_to_csv`), caminho do arquivo de saída 
(`path_to_save_file`), separador dos campos (`separator`) e tipo de arquivo de saída (`file_type`).

O uso de plugins no Airflow permite estender funcionalidades nativas, criando operadores customizados para atender necessidades 
específicas dos pipelines, como processamento de dados em diferentes formatos.
'''

dag= DAG('big_data', description='Usando operador customizado para manipulação de grandes volumes de dados',
         schedule_interval=None,
         start_date=datetime(2025, 8, 10),
         catchup=False)

big_data = BigDataOperator(
    task_id='big_data',
    path_to_csv='/opt/airflow/data/Churn.csv',
    path_to_save_file='/opt/airflow/data/Churn.parquet',
    separator=';',
    file_type='parquet',
    dag=dag
)

big_data_task = BigDataOperator(
    task_id='big_data_task',
    path_to_csv='/opt/airflow/data/Churn.csv',
    path_to_save_file='/opt/airflow/data/Churn.json',
    separator=';',
    file_type='json',
    dag=dag
)

big_data >> big_data_task