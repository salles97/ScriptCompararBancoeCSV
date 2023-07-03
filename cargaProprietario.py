from detectEncoding import detect_encoding
import pandas as pd
from psycopg2 import sql


def carga_proprietario(conexao_banco):

    # Obtém um cursor para executar comandos SQL
    cur = conexao_banco.cursor()

    # Lê o arquivo CSV em um DataFrame
    caminho_csv = '''D:/UNIFEI Aulas/0001 estagio/Projeto Itanhandu/Banco de dados/ScriptCompararBancoeCSV/carga_novas/proprietario.csv'''
    encoding = detect_encoding(caminho_csv)
    df_csv = pd.read_csv(caminho_csv, delimiter=';',
                         encoding=encoding, na_values=[""])

    print(df_csv.columns)
    for index, row in df_csv.iterrows():
        insert = sql.SQL("""
        INSERT INTO dado_antigo.proprietario (nome, cpf_cnpj, tipo, endereco, id_antigo) 
        SELECT %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM dado_antigo.proprietario 
            WHERE tipo = %s AND id_antigo = %s
        ) returning id_novo
    """)
        cur.execute(insert, (row['nome'], row['cpf_cnpj'], row['tipo'],
                             row['endereco'], row['id_antigo'], row['tipo'], row['id_antigo']))
        novoId = cur.fetchone()
        if novoId is not None:
            print('Inserido' + str(novoId))
    # Fecha a conexão com o banco de dados
    cur.close()
    conexao_banco.commit()
