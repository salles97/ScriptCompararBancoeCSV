from detectEncoding import detect_encoding
import pandas as pd
from psycopg2 import sql


def carga_logradouro(conexao_banco):
    # Estabelece a conexão com o banco de dados

    # Obtém um cursor para executar comandos SQL
    cur = conexao_banco.cursor()

    # Lê o arquivo CSV em um DataFrame
    caminho_csv = '''D:/UNIFEI Aulas/0001 estagio/Projeto Itanhandu/Banco de dados/ScriptCompararBancoeCSV/carga_novas/logradouro.csv'''
    encoding = detect_encoding(caminho_csv)
    df_csv = pd.read_csv(caminho_csv, delimiter=';',
                         encoding=encoding, na_values=[""])

    cur.execute("SELECT MAX(id_log) FROM dado_antigo.logradouro")
    max_id = cur.fetchone()[0]
    if max_id is None:
        max_id = 0
    else:
        max_id += 1000
    print(df_csv.columns)
    for index, row in df_csv.iterrows():
        # new_id = max_id + 1

        insert = sql.SQL("""
        INSERT INTO dado_antigo.logradouro (id_log, nome) 
        SELECT %s, %s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM dado_antigo.logradouro 
            WHERE nome = %s
        ) returning id_log
    """)
        cur.execute(insert, (max_id+1, row['logradouro'], row['logradouro']))
        novoId = cur.fetchone()
        if novoId is not None:
            print('Inserido' + str(novoId))
            max_id += 1

    conexao_banco.commit()
    cur.close()
