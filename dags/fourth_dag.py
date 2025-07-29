from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

'''
Estrutura da DAG, cada DAG deve possuir um nome único
Ao utilizar with nas dags, cria um contexto de execução sem a necessidade de definir a ordem
'''
with DAG('quarta_dag', description='Utilizando with ao inves de >> para setar as tasks',
         schedule_interval=None,
         start_date=datetime(2025, 7, 28),
         catchup=False) as dag:

    '''
    tasks
    O task_id é o nome da sua tarefa, que aparecerá no fluxo
    '''
    task_1= BashOperator(task_id="tsk1", bash_command="sleep 5")
    task_2= BashOperator(task_id="tsk2",bash_command="sleep 5")
    task_3= BashOperator(task_id="tsk3",bash_command="sleep 5")

    '''
    Ordem de execução das tasks
    '''
    task_1.set_upstream(task_2)
    task_2.set_upstream(task_3)