from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

'''
O DummyOperator é utilizado para estruturar o fluxo de trabalho sem executar nenhuma ação, servindo como ponto de convergência ou divisão entre tasks. 
Neste exemplo, várias tasks Bash são executadas em paralelo e, após todas concluírem, convergem para uma task dummy. 
A partir dela, o fluxo segue para outras tasks, facilitando o controle e a organização das dependências.
Task dummy não tem funcionalidade específica, é usada para estruturar o fluxo de trabalho.

Observação: O Airflow não permite criar dependências em paralelo diretamente usando o formato `[task_1, task_2, task_3] >> [task_4, task_5]`. 
Para isso, recomenda-se utilizar o DummyOperator como ponto intermediário, conforme implementado neste exemplo.

O DummyOperator é útil para criar ramos, sincronizar execuções ou marcar etapas intermediárias em pipelines complexos.
'''

dag= DAG('dag_dummy', description='DAG Dummy',
         schedule_interval=None,
         start_date=datetime(2025, 8, 9),
         catchup=False)

task_1= BashOperator(task_id="tsk1", bash_command="sleep 2", dag=dag)
task_2= BashOperator(task_id="tsk2", bash_command="sleep 2", dag=dag)
task_3= BashOperator(task_id="tsk3", bash_command="sleep 2", dag=dag)
task_4= BashOperator(task_id="tsk4", bash_command="sleep 2", dag=dag)
task_5= BashOperator(task_id="tsk5", bash_command="sleep 2", dag=dag)
task_dummy= DummyOperator(task_id="tsk_dummy", dag=dag)

[task_1, task_2, task_3] >> task_dummy >> [task_4, task_5]
