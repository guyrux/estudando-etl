# %%
import os
import zipfile
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import requests

from src.request_data import baixar_descompactar_arquivo, listar_ultimos_meses


def listar_arquivos_tabela_especifica(
    relatorio: str, tabela: str, lst_ano_mes: list,
    diretorio: str = './data/volume/'
):
    caminho_arquivo = diretorio + relatorio
    lst_arquivos = os.listdir(caminho_arquivo)

    lst_arquivos_alvo = []

    for arquivo in lst_arquivos:
        for ano_mes in lst_ano_mes:
            if (tabela in arquivo) and (ano_mes in arquivo):
                lst_arquivos_alvo.append(arquivo)

    return lst_arquivos_alvo


def listar_tabelas_especificas(
    relatorio: str, lst_ano_mes: list = [],
    diretorio: str = './data/bronze/'
):
    lst_arquivos = os.listdir(diretorio)

    lst_arquivos_alvo = []

    for arquivo in lst_arquivos:
        # for ano_mes in lst_ano_mes:
        if (relatorio in arquivo):
            lst_arquivos_alvo.append(arquivo)

    return lst_arquivos_alvo


def concatenar_tabelas(
    lst_tabelas: list = [], chaves: list = [],
    tabela_resultante: str = 'tb',
    diretorio_origem: str = './data/bronze/',
    diretorio_destino: str = './data/silver/'
):
    df_merge = pd.read_parquet(
            f'{diretorio_origem}{lst_tabelas[0]}'
        )

    for tabela in lst_tabelas[1:]:
        df_temp = pd.read_parquet(
                f'{diretorio_origem}{tabela}'
            )
        lst_colunas_iguais = list(
            set(df_temp.columns).intersection(set(df_merge.columns))
        )

        for chave in chaves:
            lst_colunas_iguais.remove(chave)

        df_temp.drop(columns=lst_colunas_iguais, inplace=True)
        df_merge = pd.merge(
            df_merge, df_temp, how='left', on=chaves
        )

    df_merge = df_merge.drop_duplicates(subset=chaves)
    df_merge.to_parquet(
        f'{diretorio_destino}{tabela_resultante}.pqt', index=False
    )


def concatenar_tabela_existente_bronze(
    relatorio: str, tabela: str,
    lst_arquivos: list
):
    df_concat = pd.DataFrame()

    for arquivo in lst_arquivos:
        df_temp = pd.read_csv(
                f'./data/volume/{relatorio}/{arquivo}', sep=";"
                , encoding='ISO-8859-1', low_memory=False
                )
        df_temp['origem'] = arquivo
        df_concat = pd.concat([
            df_concat, df_temp
            ], axis=0, ignore_index=True)
    if len(df_temp) != 0:
        df_concat.to_parquet(
            f'./data/bronze/{relatorio}_{tabela}.pqt', index=False
        )


def criar_tabela_silver(
    relatorio: str = 'Aposentados_SIAPE'
) -> None:
    pass


if __name__ == '__main__':

    # Definindo relatórios e quantidade de meses antes de M-2
    lst_relatorios = [
        'Aposentados_SIAPE', 'Pensionistas_SIAPE', 'Servidores_SIAPE'
    ]
    lst_meses = listar_ultimos_meses(n_months=2)

    # # Baixando e descompactando arquivos
    # for relatorio in lst_relatorios:
    #     for meses in lst_meses:
    #         baixar_descompactar_arquivo(month_year=meses, base_name=relatorio)

    # # Concatenando arquivos de volume para a camada bronze
    # for relatorio in lst_relatorios:
    #     for tabela in [
    #         'Cadastro', 'Observacoes', 'Remuneracao', 'Afastamento'
    #     ]:
    #         try:
    #             lst_arquivos = listar_arquivos_tabela_especifica(
    #                 relatorio=relatorio, tabela=tabela, lst_ano_mes=lst_meses
    #                 )

    #             concatenar_tabela_existente_bronze(relatorio=relatorio, tabela=tabela, lst_arquivos=lst_arquivos)
    #         except:
    #             pass

    

# %%
