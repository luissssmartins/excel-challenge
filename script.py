import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
import logging
from datetime import datetime

logging.basicConfig(filename='registros_nao_importados.log', level=logging.INFO)

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
        if pd.isna(data) or data == '':
            return None  
        return datetime.strptime(str(data), '%Y-%m-%d').date()
    except ValueError:
        return None
    
def inserir_cliente(nome, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro):
    
    try:
        data_nascimento_str = 'NULL' if data_nascimento is None else f"'{data_nascimento}'"
        data_cadastro_str = 'NULL' if data_cadastro is None else f"'{data_cadastro}'"
        
        query = (
            f"INSERT INTO tbl_clientes (nome_razao_social, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro) "
            f"VALUES ('{nome}', '{nome_fantasia}', '{cpf_cnpj}', {data_nascimento_str}, {data_cadastro_str})"
        )

        if not cliente_existe(cpf_cnpj):
            session.execute(query)
            session.commit()
            return cliente_existe(cpf_cnpj)[0]  
        else:
            return cliente_existe(cpf_cnpj)[0]  
    except exc.SQLAlchemyError as e:
        session.rollback()
        logging.info(f"Erro ao inserir cliente com CPF/CNPJ {cpf_cnpj}: {e}")
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
        logging.info(f"Erro ao inserir contato do cliente ID {cliente_id}: {e}")

def inserir_contrato(cliente_id, plano_id, vencimento, status_id, isento, endereco, numero, complemento, bairro, cep, cidade, uf):
    try:
        session.execute(
            f"INSERT INTO tbl_cliente_contratos (cliente_id, plano_id, dia_vencimento, status_id, isento, "
            f"endereco_logradouro, endereco_numero, endereco_complemento, endereco_bairro, endereco_cep, endereco_cidade, endereco_uf) "
            f"VALUES ({cliente_id}, {plano_id}, {vencimento}, {status_id}, {isento}, '{endereco}', '{numero}', "
            f"'{complemento}', '{bairro}', '{cep}', '{cidade}', '{uf}')"
        )
        session.commit()
    except exc.SQLAlchemyError as e:
        session.rollback()
        logging.info(f"Erro ao inserir contrato do cliente ID {cliente_id}: {e}")

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
        data_nascimento = validar_data(row.get('Data Nascimento'))
        data_cadastro = validar_data(row.get('Data Cadastro cliente'))
        
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
            registros_nao_importados.append((row, motivo))
            continue

        cliente_id = inserir_cliente(nome, nome_fantasia, cpf_cnpj, data_nascimento, data_cadastro)

        if cliente_id is None:
            motivo = f"Erro ao inserir ou duplicidade no CPF/CNPJ {cpf_cnpj}"
            registros_nao_importados.append((row, motivo))
            continue

        registros_importados += 1

        if not pd.isna(celulares):
            inserir_contato(cliente_id, 1, celulares)  
        if not pd.isna(telefones):
            inserir_contato(cliente_id, 2, telefones)  
        if not pd.isna(emails):
            inserir_contato(cliente_id, 3, emails)  

        plano_id = get_plano_id(plano)  
        status_id = get_status_id(status)

        inserir_contrato(cliente_id, plano_id, vencimento, status_id, isento, endereco, numero, complemento, bairro, cep, cidade, uf)

    print(f"Total de registros importados: {registros_importados}")
    print(f"Total de registros não importados: {len(registros_nao_importados)}")

    if registros_nao_importados:
        print("\nRegistros não importados:")
        for registro, motivo in registros_nao_importados:
            print(f"Registro: {registro}, Motivo: {motivo}")

def get_plano_id(plano):
    query = session.execute(f"SELECT id FROM tbl_planos WHERE descricao = '{plano}'")
    result = query.fetchone()
    return result[0] if result else None

def get_status_id(status):
    query = session.execute(f"SELECT id FROM tbl_status_contrato WHERE status = '{status}'")
    result = query.fetchone()
    return result[0] if result else None

arquivo_excel = '/Users/luis/Documents/Github/excel-challenge/dataset/data.xlsx'

processar_dados(arquivo_excel)
