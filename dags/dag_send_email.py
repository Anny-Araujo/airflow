from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator

'''
Esta DAG do Airflow exemplifica como configurar o envio automático de e-mails em caso de falha na execução de tarefas.

- O fluxo é composto por várias tasks Bash que simulam processos com o comando `sleep`.
- As tasks `tsk5` e `tsk6` utilizam o parâmetro `trigger_rule='none_failed'`, garantindo que só serão executadas se nenhuma das tasks anteriores falhar.
- A task responsável pelo envio do e-mail (`task_7`) utiliza o `EmailOperator`, que permite configurar destinatário, assunto e conteúdo do e-mail em HTML.
- O parâmetro `trigger_rule="one_failed"` na task de e-mail garante que ela será acionada apenas se pelo menos uma das tasks anteriores falhar.
- O conteúdo do e-mail inclui o identificador da DAG e uma mensagem orientando o usuário a verificar o Airflow.
- O dicionário `default_args` define parâmetros padrão, como tentativas de retry, destinatário do e-mail e notificações automáticas em caso de falha.

Esta estrutura é útil para monitorar pipelines e garantir que falhas sejam comunicadas rapidamente ao responsável.
'''

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 9),
    'email': ['mymail@dominio.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}
dag= DAG('dag_send_email', description='DAG enviando e-mail em caso de falha',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         default_view='graph', 
         tags=['processo', 'teste', 'pipeline'])

task_1= BashOperator(task_id="tsk1", bash_command="sleep 2", dag=dag)
task_2= BashOperator(task_id="tsk2", bash_command="sleep 2", dag=dag)
task_3= BashOperator(task_id="tsk3", bash_command="sleep 2", dag=dag)
task_4= BashOperator(task_id="tsk4", bash_command="exit 2", dag=dag) #essa task está programada para falhar
task_5= BashOperator(task_id="tsk5", bash_command="sleep 2", dag=dag, trigger_rule='none_failed') #essa task só executa se a task anterior não falhar
task_6= BashOperator(task_id="tsk6", bash_command="sleep 2", dag=dag, trigger_rule='none_failed')

task_7= EmailOperator(task_id='send_email',
                      
                    to="mymail@dominio.com",
                    subject="Airflow - Falha na execução da DAG",
                    html_content="""<h3>Houve uma falha na execução da DAG: {{ dag.dag_id }}</h3>
                                       <p>Por favor, entre no Airflow para verificar o que houve.</p>
                                       <br>""",
                    dag=dag, trigger_rule="one_failed") 

[task_1, task_2 ] >> task_3 >> task_4 
task_4 >> [task_5 , task_6, task_7]
