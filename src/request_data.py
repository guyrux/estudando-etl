import zipfile
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import requests


def baixar_descompactar_arquivo(month_year: str, base_name: str) -> None:
    # Requisitando o arquivo do site.
    url = f"""https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores/{month_year}_{base_name}.zip"""

    filename = f'./data/volume/downloaded_file.zip'

    extract_dir = f'./data/volume/{base_name}'

    response = requests.get(url, stream=True, timeout=500)

    if response.status_code == 200:
        with open(filename, 'wb') as file:
            file.write(response.content)

        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
    else:
        print(
            f'Failed to download the file. Status code: {response.status_code}'
        )


def listar_ultimos_meses(n_months: int = 2) -> tuple:
    # Obtém a data atual
    data_atual = datetime.now()
    data_m2 = data_atual - relativedelta(months=1)

    # Cria um DataFrame com as datas dos últimos dois meses
    datas = pd.date_range(end=data_m2, periods=n_months, freq='ME')

    # Extrai o ano das datas e o mês (com dois dígitos) e converte para string
    meses_anos = [data.strftime('%Y') + data.strftime('%m') for data in datas]

    return meses_anos