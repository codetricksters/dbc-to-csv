from csv import DictWriter      # Modulo de leitura e escrita do CSV
from dbfread import DBF         # Modulo para carregar o DBF
from pyreaddbc import readdbc   # Modulo para conversão do DBC em DBF
from rich.progress import track # Gera uma barra de progresso
from time import time           # Medir o tempo de exução
from typer import Typer         # Cria um programa executával (CLI)
import datetime                 # Processar data e hora
import os                       # Abrir arquivo

# Inicia o app (programa executável) com o modulo Typer
app = Typer()

# Grava dados no CSV linha a linha
def write_to_file(dbf_file:str, csv_file:str) -> None:
    # Abre o arquivo CSV sem carregar em memória no modo "append"
    with open(csv_file, 'a') as file:
        # Pega tempo de início da execução
        start = time()
        # Abre o arquivo DBF
        with DBF(dbf_file, encoding='utf-8', raw=False) as tf:
            # Total de linhas do arquivo DBF
            total = len(tf)
            # Itera por cada linha do arquivo DBF e extrai os dados
            for chunk in track(tf, description='Processando...'):
                # Crio um objeto a partir do dicionário da linha do arquivo DBF
                # e preparo para gravar no CSV
                dict_object = DictWriter(file, fieldnames=chunk.keys())
                # Grava uma linha nova no arquivo CSV aberto
                dict_object.writerow(chunk)
        # Pega o tempo final de execução
        end = time() - start 
        # Gera uma string no formato data e hora
        ftime = str(datetime.timedelta(seconds=end))
        # Imprime mensagem de sucesso
        print(f"Processado {total} {'linhas' if total > 1 else 'linha'} em {ftime}.")

# Cria um CSV só com o cabeçalho
def create_csv_from_dbf(dbf_file:str, csv_file:str) -> None:
    with DBF(dbf_file, encoding='utf-8', raw=False) as tf:
        with open(csv_file, 'w') as file:
            writer = DictWriter(file, fieldnames=tf.field_names)
            writer.writeheader()

# Cria um comando para execução no terminal para função que converte o dbc em csv
@app.command()
def dbc_to_csv(dbc_file:str, csv_file:str) -> None:
    # Lê o arquivo DBC e gera um arquivo DBF temporário
    readdbc.dbc2dbf(dbc_file, 'tmp.dbf')
    # Se o arquivo CSV existir chama a função write_to_file que grava o conteúdo
    # do DBF temporário no arquivo CSV
    if os.path.exists(csv_file):
        write_to_file(csv_file=csv_file, dbf_file='tmp.dbf')
    # Se o arquivo CSV não existir gera um arquivo apenas com o nome dos campos
    # e depois chama a função write_to_file para gravar os dados do DBF temporário
    # no CSV criado
    else:
        create_csv_from_dbf(dbf_file='tmp.dbf', csv_file=csv_file)
        write_to_file(csv_file=csv_file, dbf_file='tmp.dbf')
    # Remove o arquivo temporário
    os.remove('tmp.dbf')


if __name__ == '__main__':
    app()