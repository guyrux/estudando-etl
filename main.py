# %%
import pandas as pd
import requests
import zipfile

# %%
month_year = '202402'
base_name = 'Pensionistas_SIAPE'
# 'Aposentados_SIAPE'
# 'Pensionistas_SIAPE'
# 'Servidores_SIAPE'

url = f"""https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores/{month_year}_{base_name}.zip"""

r = requests.get(url)

filename = f'./data/volume/downloaded_file_{month_year}_{base_name}.zip'

response = r = requests.get(url, stream=True, timeout=500)

if response.status_code == 200:
    with open(filename, 'wb') as file:
        file.write(response.content)
else:
    print(f"Failed to download the file. Status code: {response.status_code}")


extract_dir = f'./data/bronze/{base_name}'

with zipfile.ZipFile(filename, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

# %%

# TENTATIVA DE PEGAR A DATA AUTOMATICAMENTE
# TENTATIVA DE PEGAR A DATA AUTOMATICAMENTE
# TENTATIVA DE PEGAR A DATA AUTOMATICAMENTE

from datetime import datetime, timedelta

def ultimos_dois_meses():
    # Obtém a data atual
    data_atual = datetime.now()

    # Cria um DataFrame com as datas dos últimos dois meses
    datas = pd.date_range(end=data_atual, periods=2, freq='M')

    # Extrai o mês e o ano das datas
    # meses_anos = [(data.month, data.year) for data in datas]

    # Extrai o mês (com dois dígitos) e o ano das datas
    meses_anos = [(data.strftime('%m'), data.year) for data in datas]
    print("datas", datas)
    print("meses_anos", meses_anos)

    # Convertendo cada elemento para inteiro
    meses_anos = [((item[0]), str(item[1])) for item in meses_anos]

    print("datas", datas)
    print("nova_lista", meses_anos)
    
    return meses_anos

# Testando a função
meses = ultimos_dois_meses()  
print("meses", meses)  
print("Últimos dois meses M-2:")
for mes, ano in meses:
    concat = f"{ano}{mes}"
    print(concat)

# %%

# LENDO O ARQUIVO CSV
# LENDO O ARQUIVO CSV
# LENDO O ARQUIVO CSV
import pandas as pd

dadosAposentados = pd.read_csv('./data/bronze/Aposentados_SIAPE/202402_Remuneracao.csv', sep=";", encoding='ISO-8859-1')
dadosPensionistas = pd.read_csv('./data/bronze/Pensionistas_SIAPE/202402_Remuneracao.csv', sep=";", encoding='ISO-8859-1')
dadosServidores = pd.read_csv('./data/bronze/Servidores_SIAPE/202402_Remuneracao.csv', sep=";", encoding='ISO-8859-1')

dadosAposentados.head()


# %%
dadosAposentados.head()
# %%


# LIMPANDO TODOS ARQUIVOS
# LIMPANDO TODOS ARQUIVOS
# LIMPANDO TODOS ARQUIVOS

dadosAposentados.info() 

# %%
dadosAposentados.dtypes 

# %%
# criando copia do data frame para não alterar o original 
dadosAposentados_pre = dadosAposentados.copy()


dadosAposentados_pre.info() 

dadosAposentados_pre.columns

# %%

# Selecionando e copiando somente os valores em reais (BRL);
dadosAposentados_pre = dadosAposentados_pre[['ANO', 'MES', 'Id_SERVIDOR_PORTAL', 'CPF', 'NOME',
       'REMUNERAÇÃO BÁSICA BRUTA (R$)', 
       'ABATE-TETO (R$)', 'GRATIFICAÇÃO NATALINA (R$)',
       'ABATE-TETO DA GRATIFICAÇÃO NATALINA (R$)', 'FÉRIAS (R$)',
       'FÉRIAS (U$)', 'OUTRAS REMUNERAÇÕES EVENTUAIS (R$)', 
       'IRRF (R$)', 'PSS/RPGS (R$)', 'DEMAIS DEDUÇÕES (R$)',
       'PENSÃO MILITAR (R$)', 'FUNDO DE SAÚDE (R$)', 
       'TAXA DE OCUPAÇÃO IMÓVEL FUNCIONAL (R$)',
       'REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)',
       'VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (R$)(*)',
       'VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (R$)(*)',
       'VERBAS INDENIZATÓRIAS PROGRAMA DESLIGAMENTO VOLUNTÁRIO  MP 792/2017 (R$)',
       'TOTAL DE VERBAS INDENIZATÓRIAS (R$)(*)']]

# %%

dadosAposentados_pre.info() 


# %%

# Dados faltantes
dadosAposentados_pre.isnull().sum() 


# %%

# Dropar somente 1 registro nulo 
dadosAposentados_pre.dropna(subset=['Id_SERVIDOR_PORTAL'], how='any', inplace=True)


# %%

# valor_decimal = pd.read_csv('./data/bronze/Aposentados_SIAPE/202402_Remuneracao.csv', sep=';', encoding='ISO-8859-1', decimal=',')
# valor_decimal.head()
# valor_decimal.info()

# %%

# Int
lst_column_int = ['ANO', 'MES', 'Id_SERVIDOR_PORTAL']

# String
lst_column_str = ['CPF', 'NOME']

# Float
lst_column_float = ['REMUNERAÇÃO BÁSICA BRUTA (R$)', 
'ABATE-TETO (R$)', 'GRATIFICAÇÃO NATALINA (R$)',
'ABATE-TETO DA GRATIFICAÇÃO NATALINA (R$)', 'FÉRIAS (R$)',
'FÉRIAS (U$)', 'OUTRAS REMUNERAÇÕES EVENTUAIS (R$)', 
'IRRF (R$)', 'PSS/RPGS (R$)', 'DEMAIS DEDUÇÕES (R$)',
'PENSÃO MILITAR (R$)', 'FUNDO DE SAÚDE (R$)', 
'TAXA DE OCUPAÇÃO IMÓVEL FUNCIONAL (R$)',
'REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)',
'VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (R$)(*)',
'VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (R$)(*)',
'VERBAS INDENIZATÓRIAS PROGRAMA DESLIGAMENTO VOLUNTÁRIO  MP 792/2017 (R$)',
'TOTAL DE VERBAS INDENIZATÓRIAS (R$)(*)']


for column in lst_column_int:
    dadosAposentados_pre[column] = dadosAposentados_pre[column].astype(int)

# %%
for column in lst_column_str:
    dadosAposentados_pre[column] = dadosAposentados_pre[column].astype(str)

# %%
for column in lst_column_float:
    dadosAposentados_pre[column] = dadosAposentados_pre[column].str.replace(',', '.')
    dadosAposentados_pre[column] = dadosAposentados_pre[column].astype(float)

dadosAposentados_pre.info()

# %%
# Exportando 
dadosAposentados_pre.to_csv('./data/silver/testeSilvia.csv', index=False)

# %%
# Gerando Arquivo Parquet
dadosAposentados_pre.to_parquet('./data/silver/testeSilvia.pqt', index=False)


# %%


# %% 
# append


