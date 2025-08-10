from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
import requests
'''
Esta DAG demonstra o uso do HttpSensor no Airflow para monitorar a disponibilidade de uma API antes de executar tarefas de 
processamento.

- O HttpSensor é utilizado para verificar periodicamente se um endpoint HTTP está acessível e respondendo conforme esperado.
- No exemplo, o sensor aguarda até que a API do PokeAPI esteja disponível, realizando tentativas a cada 5 segundos, com timeout 
de 20 segundos.
- Após a confirmação da disponibilidade da API, a task `process_data` executa uma função Python que faz uma requisição à API, 
obtém os dados do Pokémon "ditto" e exibe informações relevantes.
- O parâmetro `http_conn_id` deve estar configurado nas conexões do Airflow, apontando para a URL base da API. Também
pode ser criado via interface do Airflow em Admin > Connections.
- Esse padrão é útil para garantir que tarefas de processamento só sejam executadas quando recursos externos (como APIs) estiverem 
disponíveis, evitando falhas por indisponibilidade.

Ideal para cenários de integração com APIs, automação de consultas externas e orquestração de pipelines dependentes de serviços 
web.

'''

dag= DAG('http_sensor', description='Explorando o uso do HttpSensor no Airflow com API do PokeAPI',
         schedule_interval=None,
         start_date=datetime(2025, 8, 10),
         catchup=False)

def query_api():
    response = requests.get('https://pokeapi.co/api/v2/pokemon/ditto')
    if response.status_code == 200:
        data = response.json()
        print(f"Pokemon Name: {data['name']}")
        print(f"Pokemon ID: {data['id']}")
    else:
        print("Failed to retrieve data from API")


check_api = HttpSensor(
    task_id='check_api',
    http_conn_id='my_conection',
    endpoint='api/v2/pokemon/ditto',
    poke_interval=5,
    timeout=20,
    dag=dag
)

process_data = PythonOperator(
    task_id='process_data',python_callable=query_api, dag=dag)

check_api >> process_data