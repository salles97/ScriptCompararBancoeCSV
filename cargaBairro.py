import chardet
import pandas as pd
from psycopg2 import sql
from detectEncoding import detect_encoding


def carga_bairro(conexao_banco):
    cur = conexao_banco.cursor()
    # Lê o arquivo CSV em um DataFrame
    caminho_csv = '''D:/UNIFEI Aulas/0001 estagio/Projeto Itanhandu/Banco de dados/ScriptCompararBancoeCSV/carga_novas/bairro.csv'''
    encoding = detect_encoding(caminho_csv)
    df_csv = pd.read_csv(caminho_csv, delimiter=';',
                         encoding=encoding, na_values=[""])

    print(df_csv.columns)
    for index, row in df_csv.iterrows():
        insert = sql.SQL("""
        INSERT INTO dado_antigo.bairro (id_bairro, nome_bairro) 
        SELECT %s, %s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM dado_antigo.bairro 
            WHERE id_bairro = %s
        ) returning id_bairro
    """)
        cur.execute(insert, (row['id_bairro'],
                    row['nome_bairro'], row['id_bairro']))
        novoId = cur.fetchone()
        if novoId is not None:
            print('Inserido ' + str(novoId))

    conexao_banco.commit()
    cur.close()
