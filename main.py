import chardet
import csv
import pandas as pd
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
    cursor = conexao_banco.cursor()

    # Executa a consulta SQL no banco de dados
    sql_query = "SELECT id FROM dado_antigo.lote union select id FROM dado_antigo.unidade_imobiliaria order by id"
    cursor.execute(sql_query)
    # Obtém os resultados da consulta
    resultados = cursor.fetchall()

    # Cria um conjunto com os IDs presentes no banco de dados
    ids_banco = {str(result[0]) for result in resultados}
    print(ids_banco)
    # Lê o arquivo CSV em um DataFrame
    caminho_csv = '''D:/UNIFEI Aulas/0001 estagio/Projeto Itanhandu/Cadastro Imobiliario/Dados da prefeitura/CAD_IMOVEIS_06_23.csv'''
    encoding = detect_encoding(caminho_csv)
    df_csv = pd.read_csv(caminho_csv, delimiter=';',
                         encoding=encoding, na_values=[""])

    print(df_csv.columns)

    # Filtra as linhas do DataFrame que não estão presentes no banco de dados
    df_diferenca = df_csv[~df_csv['Imovel'].astype(str).isin(
        ids_banco)].drop_duplicates(subset=['Imovel'])

    # Salva a diferença em um novo arquivo CSV
    caminho_novo_csv = './imoveis_Itanhandu_faltantes.csv'
    df_diferenca.to_csv(caminho_novo_csv, index=False, sep=';')

    # Fecha a conexão com o banco de dados
    cursor.close()
    conexao_banco.close()


connectDB()
