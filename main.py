import json
import os
from models import *
import pandas as pd
from models.DatabaseConnection import DatabaseConnection
import re

path_base_cr_pend = r"/mnt/administrativo/GESTÃO/Encerramento/db_export/base_cr.xlsx"
path_base_acoes_conc = r"/mnt/administrativo/GESTÃO/Encerramento/db_export/acoes_conc.xlsx"

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


query_enc = "SELECT NOTAS_NUM_NS, USUARIOS_NOM, ACOES_OBS as OBS_ENC FROM vBIAcoes WHERE ACOES_DAT_CONCLUSAO IS NULL AND TSERVICOS_CT_COD='0915' AND TACOES_DES='EFETUAR O FECHAMENTO DA PASTA'"
query_prud = "SELECT NOTAS_NUM_NS, TACOES_DES, ACOES_DAT_CONCLUSAO, ACOES_OBS as OBS_PRUD FROM vBIAcoes WHERE TSERVICOS_CT_COD='0915' AND TACOES_DES=N'ANALISAR PRUDÊNCIA'"
query_enc_conc = "SELECT NOTAS_NUM_NS, USUARIOS_NOM, ACOES_DAT_CONCLUSAO, ACOES_OBS FROM vBIAcoes WHERE ACOES_DAT_CONCLUSAO IS NOT NULL AND TSERVICOS_CT_COD='0915' AND TACOES_DES='EFETUAR O FECHAMENTO DA PASTA'"

AcoesPendEnc = pd.read_sql(query_enc, database_connection.engine)
AcoesPendPrud = pd.read_sql(query_prud, database_connection.engine)
AcoesEncConc = pd.read_sql(query_enc_conc, database_connection.engine)

AcoesPendPrud["ACOES_DAT_CONCLUSAO"] = pd.to_datetime(AcoesPendPrud["ACOES_DAT_CONCLUSAO"], errors="coerce")

AcoesPendPrud["ACOES_DAT_CONCLUSAO_TXT"] = (AcoesPendPrud["ACOES_DAT_CONCLUSAO"].dt.strftime("%d/%m/%Y").fillna("PENDENTE"))
AcoesPendPrud["DESCRICAO"] = (AcoesPendPrud["TACOES_DES"].astype(str) + " - CONCLUSAO " + AcoesPendPrud["ACOES_DAT_CONCLUSAO_TXT"])

database_connection.__del__()

base_notas = (coletores_criticidade.groupby(["NUM_NS", "COD_POLO_CTRAB", "MODALIDADE", "PRAZO_ENTE", "CRITICIDADE"], as_index=False, dropna=False)["BASE_CR"].sum(min_count=1).rename(columns={"BASE_CR": "SOMA_BASE_CR"}))

csv_db_base_cr_path = r"./database/db_base_cr.csv"

novas_linhas = (base_notas.loc[:, ["NUM_NS", "SOMA_BASE_CR"]].copy().assign(DATA=pd.Timestamp.today().strftime("%d/%m/%Y")))

if os.path.exists(csv_db_base_cr_path) and os.path.getsize(csv_db_base_cr_path) > 0:
    existente = pd.read_csv(csv_db_base_cr_path, dtype={"NUM_NS": "int64"})
    existente["SOMA_BASE_CR"] = pd.to_numeric(existente["SOMA_BASE_CR"], errors="coerce")
    existente["DATA"] = existente["DATA"].astype(str)

    todos = pd.concat([existente, novas_linhas], ignore_index=True)
else:
    todos = novas_linhas

todos = todos.drop_duplicates(subset=["NUM_NS", "SOMA_BASE_CR", "DATA"], keep="first")

tmp_path = csv_db_base_cr_path + ".tmp"
todos.to_csv(tmp_path, index=False)
os.replace(tmp_path, csv_db_base_cr_path)


base_notas = pd.merge(base_notas, AcoesPendEnc, how="left", left_on="NUM_NS", right_on="NOTAS_NUM_NS")
base_notas.drop(columns=["NOTAS_NUM_NS"], inplace=True)

base_notas = pd.merge(base_notas, AcoesPendPrud, how="left", left_on="NUM_NS", right_on="NOTAS_NUM_NS")
base_notas.drop(columns=["NOTAS_NUM_NS", "TACOES_DES", "ACOES_DAT_CONCLUSAO", "ACOES_DAT_CONCLUSAO_TXT"], inplace=True)

base_notas.rename(columns={"COD_POLO_CTRAB": "POLO", "DESCRICAO": "PRUDENCIA", "USUARIOS_NOM": "RESPONSÁVEL 0915"}, inplace=True)
base_notas["RESPONSÁVEL 0915"] = base_notas["RESPONSÁVEL 0915"].fillna("VERIFICAR")

regionais = [
    ("DVL", "SL/DV"),
    ("FMA", "SL/DV"),
    ("PRS", "SL/DV"),
    ("PSO", "SL/VA"),
    ("BDP", "TR/PM"),
    ("IAN", "MQ/SJ")
]

df_regionais = pd.DataFrame(regionais, columns=["POLO", "REGIONAL"])

base_notas = pd.merge(base_notas, df_regionais, how="left", on="POLO")
ordem = ["NUM_NS", "POLO", "REGIONAL", "MODALIDADE", "PRAZO_ENTE", "CRITICIDADE", "SOMA_BASE_CR", "RESPONSÁVEL 0915", "OBS_ENC", "PRUDENCIA", "OBS_PRUD"]
base_notas = base_notas[ordem]
base_notas.to_excel(path_base_cr_pend, index=False)

db_base_cr = pd.read_csv(csv_db_base_cr_path, dtype={"NUM_NS": "int64"})
db_base_cr["SOMA_BASE_CR"] = pd.to_numeric(db_base_cr["SOMA_BASE_CR"], errors="coerce")
db_base_cr["DATA_DT"] = pd.to_datetime(db_base_cr["DATA"], format="%d/%m/%Y", errors="coerce")

db_base_cr = db_base_cr.dropna(subset=["DATA_DT"])

db_base_cr = db_base_cr.sort_values(
    ["DATA_DT", "NUM_NS"], kind="mergesort", ignore_index=True
)

left = AcoesEncConc.copy().rename(columns={"NOTAS_NUM_NS": "NUM_NS"})
left["ACOES_DAT_CONCLUSAO_DT"] = pd.to_datetime(left["ACOES_DAT_CONCLUSAO"], errors="coerce")

left = left.sort_values(["ACOES_DAT_CONCLUSAO_DT", "NUM_NS"], kind="mergesort", ignore_index=True)

acoes_conc = pd.merge_asof(
    left,
    db_base_cr[["NUM_NS", "SOMA_BASE_CR", "DATA_DT"]],
    by="NUM_NS",
    left_on="ACOES_DAT_CONCLUSAO_DT",
    right_on="DATA_DT",
    direction="backward",
    allow_exact_matches=False
)

acoes_conc = acoes_conc.sort_values("ACOES_DAT_CONCLUSAO_DT", ascending=False)
acoes_conc["ACOES_DAT_CONCLUSAO"] = acoes_conc["ACOES_DAT_CONCLUSAO_DT"].dt.strftime("%d/%m/%Y")
acoes_conc = acoes_conc.drop(columns=["ACOES_DAT_CONCLUSAO_DT", "DATA_DT"])

acoes_conc.to_excel(path_base_acoes_conc, index=False)

acoes_conc = acoes_conc.copy()
acoes_conc["SOMA_BASE_CR"] = pd.to_numeric(acoes_conc["SOMA_BASE_CR"], errors="coerce")
acoes_conc["ACOES_DAT_CONCLUSAO_DT"] = pd.to_datetime(
    acoes_conc["ACOES_DAT_CONCLUSAO"], format="%d/%m/%Y", errors="coerce"
)

# ===== 1) CONCLUSÕES POR DIA =====
conclusoes_diario = (
    acoes_conc.dropna(subset=["ACOES_DAT_CONCLUSAO_DT"])
    .groupby(["USUARIOS_NOM", "ACOES_DAT_CONCLUSAO_DT"], as_index=False)
    .agg(
        qtd_ns_concl=("NUM_NS", "nunique"),   # contagem de NS distintas
        base_cr_concl=("SOMA_BASE_CR", "sum") # soma base_cr nas conclusões do dia
    )
    .rename(columns={"ACOES_DAT_CONCLUSAO_DT": "DATA"})
)

# ===== 2) TRABALHO POR DIA (regex em ACOES_OBS) =====
# casa dd/mm e opcionalmente /aaaa
pat = re.compile(r"(?<!\d)(\d{1,2})/(\d{1,2})(?:/(\d{4}))?(?!\d)")

hoje = pd.Timestamp.today()

registros = []
for _, row in acoes_conc.iterrows():
    dt_conc = row["ACOES_DAT_CONCLUSAO_DT"]  # pode ser NaT
    dt_ref = dt_conc if pd.notna(dt_conc) else hoje  # ano de referência

    obs = row.get("ACOES_OBS", None)
    if pd.isna(obs):
        continue

    for m in pat.finditer(str(obs)):
        d = int(m.group(1))
        mth = int(m.group(2))
        ano_match = m.group(3)  # None se veio só dd/mm

        # sanity check básico
        if not (1 <= d <= 31 and 1 <= mth <= 12):
            continue

        if ano_match:
            # já veio o ano completo em ACOES_OBS -> usa direto
            ano = int(ano_match)
        else:
            # sem ano explícito: aplica regra
            if pd.notna(dt_conc):
                # se dd/mm observado é posterior ao dd/mm da conclusão -> ano = ano_conc - 1
                ano = dt_ref.year - 1 if (mth, d) > (dt_ref.month, dt_ref.day) else dt_ref.year
            else:
                # conclusão vazia -> usar ano de hoje (sem "ano-1")
                ano = dt_ref.year

        # valida a data (pula casos como 31/02)
        try:
            dt_trab = pd.Timestamp(year=ano, month=mth, day=d)
        except ValueError:
            continue

        registros.append({
            "USUARIOS_NOM": row["USUARIOS_NOM"],
            "DATA": dt_trab,
            "NUM_NS": row["NUM_NS"],
            "SOMA_BASE_CR": row["SOMA_BASE_CR"],
        })

df_trab = pd.DataFrame(registros)

if not df_trab.empty:
    # evita duplo cômputo da mesma NS no mesmo dia/usuário
    df_trab = df_trab.drop_duplicates(subset=["USUARIOS_NOM", "DATA", "NUM_NS"])

    trab_diario = (
        df_trab.groupby(["USUARIOS_NOM", "DATA"], as_index=False)
        .agg(
            qtd_ns_trab=("NUM_NS", "nunique"),
            base_cr_trab=("SOMA_BASE_CR", "sum")
        )
    )
else:
    trab_diario = pd.DataFrame(columns=["USUARIOS_NOM", "DATA", "qtd_ns_trab", "base_cr_trab"])

# ===== 3) RESUMO (outer join) =====
resumo = (
    conclusoes_diario.merge(trab_diario, on=["USUARIOS_NOM", "DATA"], how="outer")
    .fillna({"qtd_ns_concl": 0, "base_cr_concl": 0, "qtd_ns_trab": 0, "base_cr_trab": 0})
)
for c in ["qtd_ns_concl", "qtd_ns_trab"]:
    resumo[c] = resumo[c].astype("int64")

resumo = resumo.sort_values(["USUARIOS_NOM", "DATA"]).reset_index(drop=True)
resumo["DATA"] = resumo["DATA"].dt.strftime("%d/%m/%Y")
resumo.rename(columns={"DATA": "Data", "USUARIOS_NOM": "Usuário", "qtd_ns_concl": "NS Concluídas", "base_cr_concl": "Base Cr. Concluída", "qtd_ns_trab": "NS Trabalhadas", "base_cr_trab": "Base Cr. Trabalhada"}, inplace=True)
resumo.to_excel(r"./export/resumo.xlsx", index=False)