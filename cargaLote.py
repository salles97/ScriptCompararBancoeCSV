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
    conexao_banco = psycopg2.connect(
        host=db_config["host"],
        database=db_config["database"],
        user=db_config["user"],
        password=db_config["password"]
    )

    cur = conexao_banco.cursor()

    caminho_csv = '''D:/UNIFEI Aulas/0001 estagio/Projeto Itanhandu/Banco de dados/ScriptCompararBancoeCSV/carga_novas/lote.csv'''
    encoding = detect_encoding(caminho_csv)
    df_csv = pd.read_csv(caminho_csv, delimiter=';',
                         encoding=encoding, na_values=[""])
    df_csv = df_csv.where(pd.notnull(df_csv), None)

    print(df_csv.columns)
    for index, row in df_csv.iterrows():
        cur.execute(
            "SELECT id_novo FROM dado_antigo.proprietario WHERE nome = %s", (row['proprietario_id'],))
        proprietario_id = cur.fetchone()
        if proprietario_id is None:
            print('Proprietario não encontrado: ' + row['proprietario_id'])
            continue
        else:
            proprietario_id = proprietario_id[0]

         # Tratamento para o campo valor_venal_informado
        valor_venal_informado = row['valor_venal_informado']
        valor_venal_informado = valor_venal_informado.replace('.', '')
        valor_venal_informado = valor_venal_informado.replace(',', '.')
        valor_venal_informado = float(valor_venal_informado)

        area_total = row['area_total']
        area_total = area_total.replace('.', '')
        area_total = area_total.replace(',', '.')
        area_total = float(area_total)

        fracao_ideal = row['fracao_ideal']
        fracao_ideal = fracao_ideal.replace('.', '')
        fracao_ideal = fracao_ideal.replace(',', '.')
        fracao_ideal = float(fracao_ideal)

        insert = sql.SQL("""
        INSERT INTO dado_antigo.lote (reduzido, distrito_cod, setor_cod, quadra_cod, lote_cod, unidade, vago, 
        ocupacao, patrimonio, area_total, fracao_ideal, proprietario_id, endereco_id, valor_venal_informado, predial) 
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM dado_antigo.lote 
            WHERE reduzido = %s
        ) returning reduzido
        """)
        cur.execute(insert, (row['reduzido'], row['distrito_cod'], row['setor_cod'], row['quadra_cod'], row['lote_cod'],
                             row['unidade'], row['vago'], row['ocupacao'], row['patrimonio'], area_total,
                             fracao_ideal, proprietario_id, row['endereco_id'], valor_venal_informado, row['predial'], row['reduzido']))
        novoId = cur.fetchone()
        if novoId is not None:
            print('Inserido ' + str(novoId))

    conexao_banco.commit()
    cur.close()
    conexao_banco.close()


connectDB()
