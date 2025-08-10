from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

'''
Esta DAG exemplifica o conceito de producer no Airflow, utilizando o recurso de Datasets.

- Uma DAG producer é responsável por criar ou atualizar um recurso externo, como um arquivo, tabela ou dataset.
- Neste exemplo, a função `meu_arquivo` lê um arquivo CSV original e salva uma nova versão em outro caminho.
- O parâmetro `outlets` do PythonOperator indica que a task produz (atualiza) o dataset especificado, permitindo que outras DAGs 
(consumers) sejam disparadas automaticamente quando esse arquivo for criado ou modificado.
- Esse padrão facilita a orquestração de pipelines reativos, onde DAGs dependem da produção ou atualização de dados por outras 
DAGs.

Ideal para cenários em que é necessário garantir que etapas subsequentes só sejam executadas após a geração ou atualização de um 
recurso específico.
'''

dag= DAG('producer', description='Dataset Producer',
         schedule_interval=None,
         start_date=datetime(2025, 8, 9),
         catchup=False)

new_dataset = Dataset("/opt/airflow/data/Churn_new.csv")

def meu_arquivo():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
    dataset.to_csv("/opt/airflow/data/Churn_new.csv", sep=";", index=False)

task_producer = PythonOperator(
    task_id='task_producer', python_callable=meu_arquivo, 
    dag=dag, 
    outlets=[new_dataset])

task_producer