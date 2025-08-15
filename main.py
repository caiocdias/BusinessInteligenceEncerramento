import json
import os

from models import *
import pandas as pd

from models.DatabaseConnection import DatabaseConnection

coletores_criticidade = pd.read_excel(r"\\192.168.10.5\projetos\0915 -Encerramento\BaseDados\COLETORES DE CRITICIDADE.xlsx")

coletores_criticidade = coletores_criticidade[(coletores_criticidade['NUM_ORDEM'].str.contains('Filtro') == False)]

#Definição de Tipos
coletores_criticidade['NUM_ORDEM'] = coletores_criticidade['NUM_ORDEM'].fillna(0).astype('int64')
coletores_criticidade["GERÊNCIA NOVA"] = coletores_criticidade["GERÊNCIA NOVA"].astype('str')
coletores_criticidade["TIPO_OM"] = coletores_criticidade["TIPO_OM"].astype('str')
coletores_criticidade["NUM_NS"] = coletores_criticidade["NUM_NS"].astype('int64')
coletores_criticidade["TEXTO_BREVE"] = coletores_criticidade["TEXTO_BREVE"].astype('str')
coletores_criticidade["AREA_RESP"] = coletores_criticidade["AREA_RESP"].astype('str')
coletores_criticidade["COD_AREA_OPER"] = coletores_criticidade["COD_AREA_OPER"].astype('str')
coletores_criticidade["VAL_REALIZ"] = coletores_criticidade["VAL_REALIZ"].fillna(0).astype('int64')
coletores_criticidade["BASE_CR"] = coletores_criticidade["BASE_CR"].replace(',', '.').astype('float64')
coletores_criticidade["VAL_PFC"] = coletores_criticidade["VAL_PFC"].fillna(0).replace(',', '.').astype('float64')
coletores_criticidade["VAL_CUSTO_ESTIM"] = coletores_criticidade["VAL_CUSTO_ESTIM"].fillna(0).astype('int64')
coletores_criticidade["CUSTO_OBRA"] = coletores_criticidade["CUSTO_OBRA"].replace(',', '.').astype('float64')
coletores_criticidade["DATA_ENERGI"] = pd.to_datetime(coletores_criticidade["DATA_ENERGI"]).dt.strftime("%d/%m/%Y")
coletores_criticidade["PRAZO_ENTE"] = pd.to_datetime(coletores_criticidade["PRAZO_ENTE"]).dt.strftime("%d/%m/%Y")
coletores_criticidade["COD_PEP1"] = coletores_criticidade["COD_PEP1"].astype('str')
coletores_criticidade["COD_POLO_CTRAB"] = coletores_criticidade["COD_POLO_CTRAB"].astype('str')
coletores_criticidade["ANTIGA_MD"] = coletores_criticidade["ANTIGA_MD"].astype('str')
coletores_criticidade["MODALIDADE"] = coletores_criticidade["MODALIDADE"].astype('str')
coletores_criticidade["DATA_CRIACAO_ORDEM"] = pd.to_datetime(coletores_criticidade["DATA_CRIACAO_ORDEM"]).dt.strftime("%d/%m/%Y")
coletores_criticidade["DAT_CONC_OBRA"] = pd.to_datetime(coletores_criticidade["DAT_CONC_OBRA"]).dt.strftime("%d/%m/%Y")
coletores_criticidade["CRITICIDADE"] = coletores_criticidade["CRITICIDADE"].astype('str')
coletores_criticidade["COD_UNIV"] = coletores_criticidade["COD_UNIV"].fillna(0).astype('int64')
coletores_criticidade["QteColetores"] = coletores_criticidade["QteColetores"].astype('int64')

with open(r"./access/database.json", "r", encoding="utf-8-sig") as file:
    access_json_database = json.load(file)

database_connection = DatabaseConnection(access_json_database["username"], access_json_database["password"], access_json_database["server"], access_json_database["database"])
database_connection.connect()


query_enc = "SELECT NOTAS_NUM_NS, USUARIOS_NOM, ACOES_OBS FROM vBIAcoes WHERE ACOES_DAT_CONCLUSAO IS NULL AND TSERVICOS_CT_COD='0915' AND TACOES_DES='EFETUAR O FECHAMENTO DA PASTA'"
query_prud = "SELECT NOTAS_NUM_NS, TACOES_DES, ACOES_DAT_CONCLUSAO FROM vBIAcoes WHERE TSERVICOS_CT_COD='0915' AND TACOES_DES=N'ANALISAR PRUDÊNCIA'"
AcoesPendEnc = pd.read_sql(query_enc, database_connection.engine)
AcoesPendPrud = pd.read_sql(query_prud, database_connection.engine)

AcoesPendPrud["ACOES_DAT_CONCLUSAO"] = pd.to_datetime(
    AcoesPendPrud["ACOES_DAT_CONCLUSAO"], errors="coerce"
)

AcoesPendPrud["ACOES_DAT_CONCLUSAO_TXT"] = (AcoesPendPrud["ACOES_DAT_CONCLUSAO"].dt.strftime("%d/%m/%Y").fillna("PENDENTE"))
AcoesPendPrud["DESCRICAO"] = (AcoesPendPrud["TACOES_DES"].astype(str) + " - CONCLUSAO " + AcoesPendPrud["ACOES_DAT_CONCLUSAO_TXT"])

database_connection.__del__()

base_notas = (coletores_criticidade.groupby(["NUM_NS", "COD_POLO_CTRAB", "MODALIDADE", "PRAZO_ENTE", "CRITICIDADE"], as_index=False, dropna=False)["BASE_CR"].sum(min_count=1).rename(columns={"BASE_CR": "SOMA_BASE_CR"}))

csv_path = r"./database/db_base_cr.csv"

novas_linhas = (
    base_notas.loc[:, ["NUM_NS", "SOMA_BASE_CR"]].copy()
    .assign(DATA=pd.Timestamp.today().strftime("%d/%m/%Y"))
)

if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
    existente = pd.read_csv(csv_path, dtype={"NUM_NS": "int64"})
    existente["SOMA_BASE_CR"] = pd.to_numeric(existente["SOMA_BASE_CR"], errors="coerce")
    existente["DATA"] = existente["DATA"].astype(str)

    todos = pd.concat([existente, novas_linhas], ignore_index=True)
else:
    todos = novas_linhas

todos = todos.drop_duplicates(subset=["NUM_NS", "SOMA_BASE_CR", "DATA"], keep="first")

tmp_path = csv_path + ".tmp"
todos.to_csv(tmp_path, index=False)
os.replace(tmp_path, csv_path)


base_notas = pd.merge(base_notas, AcoesPendEnc, how="left", left_on="NUM_NS", right_on="NOTAS_NUM_NS")
base_notas.drop(columns=["NOTAS_NUM_NS"], inplace=True)

base_notas = pd.merge(base_notas, AcoesPendPrud, how="left", left_on="NUM_NS", right_on="NOTAS_NUM_NS")
base_notas.drop(columns=["NOTAS_NUM_NS", "TACOES_DES", "ACOES_DAT_CONCLUSAO", "ACOES_DAT_CONCLUSAO_TXT"], inplace=True)

base_notas.rename(columns={"COD_POLO_CTRAB": "POLO", "DESCRICAO": "PRUDENCIA", "ACOES_OBS": "OBS ENCERRAMENTO", "USUARIOS_NOM": "RESPONSÁVEL 0915"}, inplace=True)

base_notas.to_excel(r"./export/base_cr.xlsx", index=False)
