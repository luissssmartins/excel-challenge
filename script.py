import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime

logging.basicConfig(filename='erros.log', level=logging.INFO)

DATABASE_URI = 'postgresql+psycopg2://postgres:U5nhTr6fCx2KZ4dEtSbVRj@localhost:5432/challenge'

engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

def cliente_existe(cpf_cnpj):
    query = session.execute(f"SELECT id FROM tbl_clientes WHERE cpf_cnpj = '{cpf_cnpj}'")
    result = query.fetchone()
    return result

def validar_data(data):
    try:
        if pd.isna(data):
            return None  
        return pd.to_datetime(data).strftime("%Y-%m-%d")
    except ValueError:
        return None
    
def inserir_cliente(nome, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro):

    try:

        data_nascimento = validar_data(data_nascimento)
        data_cadastro = validar_data(data_cadastro)

        query = (
            "INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro) "
            "VALUES (:nome, :nome_fantasia, :cpf_cnpj, :data_nascimento, :data_cadastro)"
        )

        params = {
            'nome': nome,
            'nome_fantasia': nome_fantasia,
            'cpf_cnpj': cpf_cnpj,
            'data_nascimento': data_nascimento if data_nascimento else None,
            'data_cadastro': data_cadastro if data_cadastro else None
        }

        if not cliente_existe(cpf_cnpj):

            session.execute(query, params)
            session.commit()

            return cliente_existe(cpf_cnpj)[0]  
        else:
            return cliente_existe(cpf_cnpj)[0]  

    except exc.SQLAlchemyError as e:
        session.rollback()
        logging.info(f"Erro ao inserir cliente com CPF/CNPJ {cpf_cnpj}, motivo: {e}")
        return None

def inserir_plano(descricao_plano, valor_plano):
    try:
        query = "SELECT id FROM tbl_planos WHERE descricao = :descricao_plano"
        result = session.execute(query, {'descricao_plano': descricao_plano}).fetchone()
        
        if result:
            return result[0]
        
        insert_query = "INSERT INTO tbl_planos (descricao, valor) VALUES (:descricao, :valor) RETURNING id"
        result = session.execute(insert_query, {'descricao': descricao_plano, 'valor': valor_plano}).fetchone()
        
        session.commit()
        
        return result[0] if result else None
    
    except exc.SQLAlchemyError as e:
        session.rollback()
        logging.info(f"Erro ao inserir o plano '{descricao_plano}', motivo: {e}")
        return None


def inserir_contato(cliente_id, tipo_contato_id, contato):
    try:
        session.execute(
            f"INSERT INTO tbl_cliente_contatos (cliente_id, tipo_contato_id, contato) "
            f"VALUES ({cliente_id}, {tipo_contato_id}, '{contato}')"
        )
        session.commit()
    except exc.SQLAlchemyError as e:
        session.rollback()
        logging.info(f"Erro ao inserir contato do cliente ID {cliente_id}, motivo: {e}")

def inserir_contrato(cliente_id, plano_id, vencimento, status_id, isento, endereco, numero, complemento, bairro, cep, cidade, uf):
    
    try:
        if plano_id is None:
            logging.info(f"Contrato do cliente {cliente_id} não pode ser inserido. Plano não encontrado.")
            return

        session.execute(
            f"""
            INSERT INTO tbl_cliente_contratos (
                cliente_id, plano_id, dia_vencimento, status_id, "isento", 
                endereco_logradouro, endereco_numero, endereco_complemento, endereco_bairro, endereco_cep, endereco_cidade, endereco_uf
            ) VALUES (
                {cliente_id}, {plano_id}, {vencimento}, {status_id}, {isento}, 
                '{endereco}', '{numero}', '{complemento}', '{bairro}', '{cep}', '{cidade}', '{uf}'
            )
            """
        )
        session.commit()

    except exc.SQLAlchemyError as e:
        session.rollback()
        logging.info(f"Erro ao inserir contrato do cliente ID {cliente_id}, motivo: {e}")

def processar_dados(arquivo_excel):
    try:
        df = pd.read_excel(arquivo_excel)
    except Exception as e:
        print(f"Erro ao ler o arquivo: {e}")
        return

    registros_importados = 0
    registros_nao_importados = []

    for _, row in df.iterrows():
        
        nome = row.get('Nome/Razão Social')
        nome_fantasia = row.get('Nome Fantasia')
        cpf_cnpj = row.get('CPF/CNPJ')
        data_nascimento = row.get('Data Nasc.')
        data_cadastro = row.get('Data Cadastro cliente')
        
        celulares = row.get('Celulares')
        telefones = row.get('Telefones')
        emails = row.get('Emails')
        
        endereco = row.get('Endereço')
        numero = row.get('Número')
        complemento = row.get('Complemento')
        bairro = row.get('Bairro')
        cep = row.get('CEP')
        cidade = row.get('Cidade')
        uf = row.get('UF')
        
        plano = row.get('Plano')
        plano_valor = row.get('Plano Valor')
        vencimento = row.get('Vencimento')
        status = row.get('Status')
        isento = row.get('Isento')

        if pd.isna(nome) or pd.isna(cpf_cnpj):
            motivo = f"Nome ou CPF/CNPJ ausente para o registro {row}"

            logging.info(motivo)

            registros_nao_importados.append(motivo)
            continue

        cliente_id = inserir_cliente(nome, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)

        if cliente_id is None:
            motivo = f"Erro ao inserir ou duplicidade no CPF/CNPJ {cpf_cnpj}"
            registros_nao_importados.append(motivo)
            continue

        registros_importados += 1

        if not pd.isna(celulares):
            inserir_contato(cliente_id, 1, celulares)  
        if not pd.isna(telefones):
            inserir_contato(cliente_id, 2, telefones)  
        if not pd.isna(emails):
            inserir_contato(cliente_id, 3, emails)  

        inserir_plano(plano, plano_valor)

        plano_id = get_plano_id(plano)  
        status_id = get_status_id(status)

        inserir_contrato(cliente_id, plano_id, vencimento, status_id, isento, endereco, numero, complemento, bairro, cep, cidade, uf)

    print(f"Total de registros importados: {registros_importados}")
    print(f"Total de registros não importados: {len(registros_nao_importados)}")

    if registros_nao_importados:

        print("\nRegistros não importados:")

        for motivo in registros_nao_importados:
            print(f"Motivo: {motivo}")

def get_plano_id(plano):
    query = session.execute(f"SELECT id FROM tbl_planos WHERE descricao = '{plano}'")
    result = query.fetchone()
    
    if result:
        return result[0]
    else:
        logging.info(f"Plano '{plano}' não encontrado")
        return None  

def get_status_id(status):
    query = session.execute(f"SELECT id FROM tbl_status_contrato WHERE status = '{status}'")
    result = query.fetchone()
    return result[0] if result else None

arquivo_excel = '/Users/luis/Documents/Github/excel-challenge/dataset/data.xlsx'

processar_dados(arquivo_excel)
