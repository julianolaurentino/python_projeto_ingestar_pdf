# %%
import pandas as pd
import numpy as np
import pdfplumber
import re
pd.set_option('display.max_columns', None)


caminho_pdf = r"/home/obzen/Downloads/Relatório Semanal SBFZ - 19.01.20262.pdf"

# =============================================================================
# ETAPA 1 — Extrair tabela de Partidas usando coordenadas Y
# =============================================================================
with pdfplumber.open(caminho_pdf) as pdf:
    pagina = pdf.pages[0]

    palavras = pagina.extract_words()

    # Encontrar coordenada Y do título "Partidas" (segunda ocorrência)
    y_partidas = None
    y_chegadas_partidas = None
    ocorrencias_estimativa = []


    for palavra in palavras:
        if "Partidas" in palavra["text"]:
            ocorrencias_estimativa.append(palavra["top"])

    # Pegar a primeira ocorrência de "Partidas" isolada (não "Chegadas + Partidas")
    if len(ocorrencias_estimativa) >= 1:
        y_partidas = ocorrencias_estimativa[0]
    if len(ocorrencias_estimativa) >= 2:
        y_chegadas_partidas = ocorrencias_estimativa[1]

    print(f"Y início Partidas: {y_partidas}")
    print(f"Y fim (Chegadas + Partidas): {y_chegadas_partidas}")

    if y_partidas and y_chegadas_partidas:
        # Recortar a região da tabela de Partidas
        bbox = (0, y_partidas, pagina.width, y_chegadas_partidas)
        regiao = pagina.crop(bbox)
        df_raw = regiao.extract_table()

# =============================================================================
# ETAPA 2 — Converter lista de listas para DataFrame
# =============================================================================
df_raw = pd.DataFrame(df_raw)
print(f"Shape: {df_raw.shape}")
df_raw.head(10)

# =============================================================================
# ETAPA 3 — Normalizar: None e '' → NaN
# =============================================================================
df_raw = df_raw.replace({None: np.nan, '': np.nan})

# =============================================================================
# ETAPA 4 — Extrair metadados (Ano, Mês, Dias)
# =============================================================================
def encontrar_linha(df, valor):
    """Retorna índice da linha onde o valor aparece em qualquer coluna."""
    for i, row in df.iterrows():
        if valor in row.values:
            return i
    return None

# --- Ano ---
idx_ano = encontrar_linha(df_raw, 'Ano')
ano = None
if idx_ano is not None:
    row_ano = df_raw.iloc[idx_ano].dropna()
    for v in row_ano.values:
        try:
            ano = int(float(str(v)))
            break
        except ValueError:
            continue
print(f"Ano encontrado: {ano}")

# --- Mês ---
idx_mes = encontrar_linha(df_raw, 'Mês')
mapa_mes = {}
if idx_mes is not None:
    for col_idx, val in enumerate(df_raw.iloc[idx_mes]):
        if pd.notna(val) and str(val).strip() not in ('', 'Mês'):
            mapa_mes[col_idx] = str(val).strip()
print(f"Meses encontrados: {mapa_mes}")

# --- Dias (linha com 'Hora') ---
idx_hora = encontrar_linha(df_raw, 'Hora')
mapa_dia = {}
if idx_hora is not None:
    for col_idx, val in enumerate(df_raw.iloc[idx_hora]):
        if pd.notna(val) and str(val).strip() not in ('', 'Hora'):
            try:
                mapa_dia[col_idx] = int(float(str(val).replace(',', '.')))
            except ValueError:
                pass
print(f"Dias encontrados ({len(mapa_dia)}): {list(mapa_dia.values())}")
 
# =============================================================================
# ETAPA 5 — Filtrar linhas de dados (horários HH:MM)
# =============================================================================
def is_horario(val):
    return bool(re.match(r'^\d{1,2}:\d{2}$', str(val).strip()))

# Descobre qual coluna tem os horários (primeira coluna não-nula com HH:MM)
col_hora = None
for col in df_raw.columns:
    if df_raw[col].apply(is_horario).sum() > 5:
        col_hora = col
        break

print(f"Coluna de horários: {col_hora}")
df_dados = df_raw[df_raw[col_hora].apply(is_horario)].copy()
print(f"Linhas de dados encontradas: {len(df_dados)}")

# =============================================================================
# ETAPA 6 — Wide → Long com mapa de dia/mês por coluna
# =============================================================================
def _mes_para_coluna(col_idx, mapa_mes):
    mes_atual = None
    for idx in sorted(mapa_mes.keys()):
        if idx <= col_idx:
            mes_atual = mapa_mes[idx]
    return mes_atual

registros = []
for _, row in df_dados.iterrows():
    hora = str(row[col_hora]).strip()
    for col_idx, val in enumerate(row):
        if col_idx == col_hora:
            continue
        if pd.isna(val) or str(val).strip() in ('', 'nan'):
            continue
        dia = mapa_dia.get(col_idx)
        if dia is None:
            continue
        mes = _mes_para_coluna(col_idx, mapa_mes)
        try:
            volume = int(str(val).replace('.', '').replace(',', ''))
        except ValueError:
            continue
        registros.append({'hora': hora, 'dia': dia, 'mes': mes, 'ano': ano, 'volume': volume})

df_long = pd.DataFrame(registros)
print(f"Registros após melt: {len(df_long)}")
df_long.head()

# =============================================================================
# ETAPA 7 — Enriquecer colunas
# =============================================================================
MESES = {
    'janeiro':1,'fevereiro':2,'março':3,'abril':4,
    'maio':5,'junho':6,'julho':7,'agosto':8,
    'setembro':9,'outubro':10,'novembro':11,'dezembro':12
}

df_long['mes_ref'] = df_long['mes'].str.lower().map(MESES).astype('Int64')
df_long['ano_ref'] = df_long['ano'].astype('Int64')

hora_int = df_long['hora'].str.split(':').str[0].astype(int)
df_long['turno'] = np.select(
    [hora_int.between(0,5), hora_int.between(6,11),
     hora_int.between(12,17), hora_int.between(18,23)],
    ['Madrugada','Manhã','Tarde','Noite'],
    default='Indefinido'
)

df_long['data'] = pd.to_datetime(
    df_long[['ano_ref','mes_ref','dia']].rename(
        columns={'ano_ref':'year','mes_ref':'month','dia':'day'}
    ), errors='coerce'
)
# adicionando coluna de lounge (FOR DOM)
df_long['mesclado'] = 'FOR DOM'

# =============================================================================
# ETAPA 8 — Agrupamento final
# =============================================================================
df_final = (
    df_long
    .groupby(['ano_ref','mes_ref','mes','dia','data','turno','mesclado','hora'], dropna=False)
    ['volume'].sum()
    .reset_index()
    .sort_values(['ano_ref','mes_ref','dia','hora'])
    .reset_index(drop=True)
)

print(f"DataFrame final: {df_final.shape}")
df_final.head(200)