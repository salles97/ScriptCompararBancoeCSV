import psycopg2
from config import db_config
from cargaBairro import carga_bairro
from cargaEndereco import carga_endereco
from cargaLogradouro import carga_logradouro
from cargaLote import carga_lote
from cargaProprietario import carga_proprietario


def main():
    # Estabelece a conexão com o banco de dados
    conexao_banco = psycopg2.connect(
        host=db_config["host"],
        database=db_config["database"],
        user=db_config["user"],
        password=db_config["password"]
    )
    print('Carga de proprietario')
    carga_proprietario(conexao_banco)
    print('Carga de bairro')
    carga_bairro(conexao_banco)
    print('Carga de logradouros')
    carga_logradouro(conexao_banco)
    print('Carga de enderecos')
    carga_endereco(conexao_banco)
    print('Carga de lotes')
    carga_lote(conexao_banco)

    conexao_banco.close()


main()
