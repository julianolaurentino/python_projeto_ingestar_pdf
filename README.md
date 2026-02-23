# 🐍 Projeto ETL Python em arquivos .pdf

### Tabela dentro do PDF para ingestar
![image](data\partidas.png)

### Bibliotecas Python
Essas são as bibliotecas utilizas nessa projeto:

```
import pandas as pd
import numpy as np
import pdfplumber
import re
```
### Ingestão
Aqui será realizado o tratamento de acordo com a demanda do arquivo, podendo variar em cada cenário e tipo de pdf's, mas sempre seguindo a mesma lógica:

- Achar a tabela a partir de referencias dentro do pdf (nesse caso acharemos a informação a partir da palavra "Partidas" como referềncia)
- transformar em tabela, estruturar e exportar uma base limpa para ser consumida em alguma ferramenta de data viz

### Estrutura desse arquivo
* Linha 0: Ano = 2026
 * Linha 3: Mês = janeiro / fevereiro (meses espalhados em colunas)
* Linha 6: Hora | Dias (19, 20, 21, 22... 31, 1, 2) — são os dias do mês como colunas
* Linhas 9+: Horários (00:00, 01:00, 02:00...) com volumes por dia

### Etapas
**Etapa 1** — 
Abre o PDF com pdfplumber e varre todas as palavras da página para localizar as coordenadas verticais (eixo Y) das ocorrências do termo "Partidas". A primeira ocorrência marca o início da tabela desejada e a segunda (que corresponde ao título "Chegadas + Partidas") marca o fim. Com esses dois pontos é feito um recorte preciso da região (crop) e a tabela é extraída como lista de listas via extract_table(). Louco não?
```python
with pdfplumber.open(caminho_pdf) as pdf:
    pagina = pdf.pages[0]

    palavras = pagina.extract_words()

    y_partidas = None
    y_chegadas_partidas = None
    ocorrencias_estimativa = []


    for palavra in palavras:
        if "Partidas" in palavra["text"]:
            ocorrencias_estimativa.append(palavra["top"])

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
```


**Etapa 2** — 
A lista de listas retornada pelo pdfplumber é convertida em um pd.DataFrame, tornando possível o uso de todas as ferramentas do pandas nas etapas seguintes.

```python
df_raw = pd.DataFrame(df_raw)
print(f"Shape: {df_raw.shape}")
df_raw.head(10)
```

**Etapa 3** — 
Valores None e strings vazias " " — muito comuns em extrações de PDF — são uniformizados para NaN, garantindo que as verificações de nulidade com pd.isna() funcionem de forma consistente em todo o DataFrame.

```python
df_raw = df_raw.replace({None: np.nan, '': np.nan})
```


**Etapa 4** — Como o cabeçalho da tabela é composto por múltiplas linhas espalhadas, esta etapa localiza cada informação individualmente:

* Ano: encontra a linha que contém a string 'Ano' e extrai o valor numérico ao lado.
* Mês: encontra a linha com 'Mês' e monta um dicionário {índice_coluna → nome_do_mês}, permitindo saber qual mês corresponde a cada bloco de colunas.
* Dias: encontra a linha com 'Hora' (que contém os dias do mês como cabeçalho) e monta um dicionário {índice_coluna → número_do_dia}.

```python
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
```

**Etapa 5** — Identifica automaticamente qual coluna contém os horários no formato HH:MM (usando regex) e filtra o DataFrame mantendo apenas as linhas que representam dados reais, descartando cabeçalhos, linhas em branco e metadados residuais.

```python
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
```

**Etapa 6** — Converte a tabela do formato wide (colunas = dias, linhas = horas) para o formato long (um registro por combinação hora + dia). Para cada célula com valor válido, o código consulta os dicionários da Etapa 4 para descobrir o dia e o mês corretos. Volumes são limpos de formatação (pontos de milhar, vírgulas decimais) e convertidos para inteiro. O resultado é um DataFrame com as colunas: hora, dia, mes, ano, volume.

```python
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
```

**Etapa 7** —
mes_ref: converte o nome do mês em português para o número ordinal (ex: 'janeiro' → 1).
ano_ref: garante o ano como inteiro tipado (Int64).
turno: classifica cada hora em um dos quatro turnos do dia — Madrugada (00h–05h), Manhã (06h–11h), Tarde (12h–17h) e Noite (18h–23h).
data: monta uma data completa (datetime) a partir de ano, mês e dia, útil para ordenações e análises de série temporal.
mesclado: coluna estática com o identificador do lounge de origem ('FOR DOM'), permitindo rastrear a procedência dos dados em cargas futuras com múltiplas origens.

```python
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
```

**Etapa 8** - 
Consolida os registros agrupando por todas as dimensões analíticas (ano_ref, mes_ref, mes, dia, data, turno, mesclado, hora) e somando os volumes. O resultado é ordenado cronologicamente por ano, mês, dia e hora, gerando o DataFrame final limpo, estruturado e pronto para carga ou análise.

```python
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
```
### Resultado
![Tabela final](data\resultado.png)


### Exportar
Agora exporte para algum diretório (nesse caso exportei para csv "Escolha o melhor diretório para o seu caso"):
```python
df_final.to_csv("data\partidas_processado.csv", index=False)
```