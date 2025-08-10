from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd 

"""
Este código define um operador customizado para o Airflow, chamado BigDataOperator, que permite converter arquivos CSV em 
formatos Parquet ou JSON de forma automatizada dentro de pipelines.

- As importações incluem:
    - `BaseOperator`: classe base para criação de operadores customizados no Airflow.
    - `apply_defaults`: decorador para facilitar a definição de parâmetros no construtor do operador.
    - `pandas`: biblioteca utilizada para leitura e conversão dos arquivos de dados.

- O operador recebe os parâmetros:
    - `path_to_csv`: caminho do arquivo CSV de entrada.
    - `path_to_save_file`: caminho para salvar o arquivo convertido.
    - `separator`: separador dos campos do CSV (default: ';').
    - `file_type`: formato de saída desejado ('parquet' ou 'json').

- O método `execute` realiza a leitura do CSV e converte para o formato especificado, salvando o resultado no caminho informado. 
Caso o formato seja inválido, uma exceção é lançada.

Plugins no Airflow permitem estender funcionalidades nativas, criando operadores, sensores, hooks e interfaces customizadas 
para atender necessidades específicas dos pipelines de dados.
"""

class BigDataOperator(BaseOperator):

    @apply_defaults
    def __init__(self, path_to_csv, path_to_save_file, separator=';', file_type='parquet', *args, **kwargs) -> None:
        super(BigDataOperator, self).__init__(*args, **kwargs)
        self.path_to_csv = path_to_csv
        self.path_to_save_file = path_to_save_file
        self.separator = separator
        self.file_type = file_type

    def execute(self, context):
        df = pd.read_csv(self.path_to_csv, sep=self.separator)
        if self.file_type == 'parquet':
            df.to_parquet(self.path_to_save_file, index=False)
        elif self.file_type == 'json':
            df.to_json(self.path_to_save_file)
        else:
            raise ValueError(f"File type é inválido: {self.file_type}")