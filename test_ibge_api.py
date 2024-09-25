import requests

def obter_uf_por_estado(estado):
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    try:
        response = requests.get(url)
        response.raise_for_status()

        estados = response.json()

        for estado_data in estados:
            if estado_data['nome'].strip().lower() == estado.strip().lower():
                return estado_data['sigla']

        print(f'Estado: {estado} não encontrado.')
        return None

    except requests.exceptions.RequestException as e:
        return None

def obter_uf_por_cidade(cidade):
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

    try:
        response = requests.get(url)
        response.raise_for_status()

        municipios = response.json()

        for municipio in municipios:
            if municipio['nome'].strip().lower() == cidade.strip().lower():
                return municipio['microrregiao']['mesorregiao']['UF']['sigla']

        print(f'Cidade: {cidade} não encontrada.')
        return None

    except requests.exceptions.RequestException as e:
        return None

def determinar_uf(parametro):

    uf = obter_uf_por_estado(parametro)

    if uf is not None:
        return uf

    uf = obter_uf_por_cidade(parametro)

    if uf is not None:
        return uf

    logging.info(f"Erro: Nenhum estado ou cidade válido foi encontrado para o parâmetro '{parametro}'.")
    return None

print(determinar_uf("Parnamirim"))