from detectEncoding import detect_encoding
import pandas as pd
from psycopg2 import sql


def carga_endereco(conexao_banco):
    # Estabelece a conexão com o banco de dados

    # Obtém um cursor para executar comandos SQL
    cur = conexao_banco.cursor()

    # Lê o arquivo CSV em um DataFrame
    caminho_csv = '''D:/UNIFEI Aulas/0001 estagio/Projeto Itanhandu/Banco de dados/ScriptCompararBancoeCSV/carga_novas/endereco.csv'''
    encoding = detect_encoding(caminho_csv)
    df_csv = pd.read_csv(caminho_csv, delimiter=';',
                         encoding=encoding, na_values=[""])

 # Substitui todos os valores NaN por None
    df_csv = df_csv.where(pd.notnull(df_csv), None)

    print(df_csv.columns)
    for index, row in df_csv.iterrows():

        # Consulta para encontrar o id do logradouro
        cur.execute(
            "SELECT id_log FROM dado_antigo.logradouro WHERE nome = %s", (row['logradouro'],))
        logradouro_id = cur.fetchone()
        if logradouro_id is None:
            print(logradouro_id)
            print('Logradouro não encontrado: ' + row['logradouro'])

        else:
            logradouro_id = logradouro_id[0]

        complemento = row.get('complemento')
        if pd.isna(complemento):
            complemento = ''

        if len(complemento) > 50:
            print(f"Complemento original: {complemento}")
            complemento = complemento[:50]
            print(f"Complemento truncado: {complemento}")

        loteamento = row.get('loteamento')
        if pd.isna(loteamento):
            loteamento = ''

        insert = sql.SQL("""
        INSERT INTO dado_antigo.endereco (id, logradouro, logradouro_id, numero, bairro, complemento, apartamento, loteamento, bairro_id) 
        SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 
            FROM dado_antigo.endereco 
            WHERE id = %s
        ) returning id
    """)
        cur.execute(insert, (row['id'], row['logradouro'], logradouro_id, row['numero'], row['bairro'],
                             complemento, row['apartamento'],  loteamento, row['bairro_id'], row['id']))
        novoId = cur.fetchone()
        if novoId is not None:
            print('Inserido ' + str(novoId))

    conexao_banco.commit()
    cur.close()
