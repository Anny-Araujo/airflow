from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

'''
Esta DAG demonstra o uso dos parâmetros `pool` e `priority_weight` no Airflow.

- O parâmetro `pool` permite limitar o número de tasks que podem ser executadas simultaneamente em um grupo específico, ajudando 
a controlar o uso de recursos compartilhados. 
- Para criar um pool, acesse a interface web do Airflow, vá em "Admin" > "Pools" e adicione um novo pool, definindo o nome 
(ex: "testepool") e o número máximo de slots disponíveis (nesse caso, 1 só).
- Todas as tasks desta DAG utilizam o pool "testepool", competindo pelo slot configurado.
- A propriedade `priority_weight` define a prioridade de execução das tasks dentro do pool. Tasks com maior valor de prioridade 
são executadas antes quando há concorrência por slots.
- Neste exemplo, `tsk4` tem prioridade maior que as demais, seguida por `tsk2`, enquanto `tsk1` e `tsk3` possuem prioridade padrão.

O uso de pools e prioridades é útil para gerenciar recursos limitados e garantir que tarefas críticas sejam executadas primeiro 
em ambientes concorridos.
'''

dag= DAG('dag_pool', description='DAG Pools',
         schedule_interval=None,
         start_date=datetime(2025, 8, 9),
         catchup=False)

task_1= BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag, pool="testepool")
task_2= BashOperator(task_id="tsk2", bash_command="sleep 5", dag=dag, pool="testepool", priority_weight=5)
task_3= BashOperator(task_id="tsk3", bash_command="sleep 5", dag=dag, pool="testepool")
task_4= BashOperator(task_id="tsk4", bash_command="sleep 5", dag=dag, pool="testepool", priority_weight=10)

#Se não for definida a precedência, as tasks serão executadas em paralelo