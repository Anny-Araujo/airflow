from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import statistics

'''
Esta DAG demonstra o uso do PythonOperator no Airflow para executar funções Python personalizadas durante o pipeline de dados.

No exemplo, a função `data_cleaner` realiza o pré-processamento de um arquivo CSV:
- Lê o arquivo de dados brutos.
- Renomeia as colunas para padronização.
- Preenche valores ausentes na coluna "Salario" com a mediana.
- Preenche valores ausentes na coluna "Genero" com "Masculino".
- Corrige valores inválidos na coluna "Idade" (menores que 0 ou maiores que 120) usando a mediana.
- Remove registros duplicados com base no campo "Id".
- Salva o resultado limpo em um novo arquivo CSV.

O PythonOperator permite que qualquer função Python seja executada como uma task dentro da DAG, tornando o Airflow flexível para 
tarefas de processamento, transformação ou integração de dados.
'''

dag= DAG('python_operator', description='Explorando o uso do PythonOperator no Airflow',
         schedule_interval=None,
         start_date=datetime(2025, 8, 9),
         catchup=False)

def data_cleaner():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
    dataset.columns = ["Id", "Score", "Estado", "Genero", "Idade", "Patrimônio", "Saldo",
                       "Produtos", "CartãoCrédito", "Ativo", "Salario", "Saiu"]
    
    mediana = statistics.median(dataset["Salario"])
    dataset["Salario"].fillna(mediana, inplace=True)

    dataset["Genero"].fillna("Masculino", inplace=True)

    mediana = statistics.median(dataset["Idade"])
    dataset.loc[(dataset["Idade"]< 0) | (dataset["Idade"]> 120), "Idade"] = mediana

    dataset.drop_duplicates(subset=["Id"], keep="first", inplace=True)

    dataset.to_csv("/opt/airflow/data/Churn_Cleaned.csv", sep=";", index=False)

task_python = PythonOperator(
    task_id='task_data_cleaner', python_callable=data_cleaner, dag=dag)