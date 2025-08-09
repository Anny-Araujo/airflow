from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

'''
XCom (Cross-communication) é o mecanismo do Airflow para troca de pequenos volumes de dados
entre tasks de uma DAG.

- Use XCom para compartilhar informações simples, como strings, números ou pequenos objetos.
- Para enviar dados: utilize o método `xcom_push(key, value)` dentro da task.
- Para receber dados: utilize o método `xcom_pull(task_ids, key)` na task que irá consumir.
- Recomenda-se nomear as keys de forma clara e evitar o uso de XCom para grandes volumes de dados ou arquivos.
- Exemplo:
    # Enviando dados
    kwargs['ti'].xcom_push(key='meu_resultado', value='valor')
    # Recebendo dados
    valor = kwargs['ti'].xcom_pull(task_ids='id_da_task', key='meu_resultado')

É necessário importar o PythonOperator para utilizar XCom em funções Python.
'''


dag= DAG('dag_xcom', description='DAG que utiliza o xcom',
         schedule_interval=None,
         start_date=datetime(2025, 8, 9),
         catchup=False)

def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='output', value='Isso é um teste de saída para a task_1.')

task_1= PythonOperator(task_id="tsk1", python_callable=task_write, dag=dag)

def task_read(**kwargs):
    valor = kwargs['ti'].xcom_pull(task_ids='tsk1', key='output')
    print(f"Valor recebido da task_1: {valor}")

task_2= PythonOperator(task_id="tsk2", python_callable=task_read, dag=dag)

task_1 >> task_2