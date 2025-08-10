from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

'''
Esta DAG exemplifica o conceito de consumer no Airflow, utilizando o recurso de Datasets.

- Uma DAG consumer é disparada automaticamente quando um recurso externo (dataset) é criado ou atualizado por uma DAG producer.
- Neste exemplo, a DAG é agendada para ser executada sempre que o arquivo `/opt/airflow/data/Churn_new.csv` for modificado.
- A função `meu_arquivo` lê esse arquivo e salva uma nova versão em outro caminho, mostrando como consumir e processar dados 
produzidos por outra DAG.
- O parâmetro `schedule=[new_dataset]` garante que a DAG só será executada após a atualização do dataset especificado.
- O parâmetro `provide_context=True` no PythonOperator permite que a função receba o contexto de execução do Airflow, como 
informações sobre a DAG, task, execução e XComs, tornando possível acessar dados dinâmicos durante o processamento.

Esse padrão facilita a orquestração de pipelines reativos, garantindo que etapas dependentes só sejam executadas quando os dados 
necessários estiverem disponíveis.
'''

new_dataset = Dataset("/opt/airflow/data/Churn_new.csv")

dag= DAG('consumer', description='Dataset Consumer',
         schedule=[new_dataset],
         start_date=datetime(2025, 8, 9),
         catchup=False)

def meu_arquivo():
    dataset = pd.read_csv("/opt/airflow/data/Churn_new.csv", sep=";")
    dataset.to_csv("/opt/airflow/data/Churn_new2.csv", sep=";", index=False)

task_consumer = PythonOperator(
    task_id='task_consumer', python_callable=meu_arquivo, 
    dag=dag, 
    provide_context=True)

task_consumer