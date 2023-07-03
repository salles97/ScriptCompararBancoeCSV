import chardet
import csv
import pandas as pd
from psycopg2 import sql
import psycopg2
from config import db_config


def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']


def connectDB():
    # Estabelece a conexão com o banco de dados
    conexao_banco = psycopg2.connect(
        host=db_config["host"],
        database=db_config["database"],
        user=db_config["user"],
        password=db_config["password"]
    )

    # Obtém um cursor para executar comandos SQL
    cur = conexao_banco.cursor()

    # Lê o arquivo CSV em um DataFrame
    caminho_csv = '''D:/UNIFEI Aulas/0001 estagio/Projeto Itanhandu/Banco de dados/ScriptCompararBancoeCSV/carga_novas/logradouro.csv'''
    encoding = detect_encoding(caminho_csv)
    df_csv = pd.read_csv(caminho_csv, delimiter=';',
                         encoding=encoding, na_values=[""])

    cur.execute("SELECT MAX(id_log) FROM dado_antigo.logradouros")
    max_id = cur.fetchone()[0]
    if max_id is None:
        max_id = 0
    else:
        max_id += 1000
    print(df_csv.columns)
    for index, row in df_csv.iterrows():
        # new_id = max_id + 1

        insert = sql.SQL("""
        INSERT INTO dado_antigo.logradouros (id_log, nome_log) 
        SELECT %s, %s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM dado_antigo.logradouros 
            WHERE nome_log = %s
        ) returning id_log
    """)
        cur.execute(insert, (max_id+1, row['logradouro'], row['logradouro']))
        novoId = cur.fetchone()
        if novoId is not None:
            print('Inserido' + str(novoId))
            max_id += 1

    conexao_banco.commit()
    cur.close()
    conexao_banco.close()


connectDB()
