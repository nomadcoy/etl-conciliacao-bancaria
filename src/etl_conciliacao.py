from dotenv import load_dotenv
import os

load_dotenv()

import pyodbc
import pandas as pd
from datetime import date, datetime
from google.cloud import bigquery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCP_CREDENTIALS")

# ---------------------------
# Conexão SQL Server (Sankhya)
# ---------------------------
sql_conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    f"Server={os.getenv('DB_SERVER')};"
    f"Database={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
)
conn = pyodbc.connect(sql_conn_str)
cursor = conn.cursor()

# ---------------------------
# Conexão BigQuery
# ---------------------------
bq_client = bigquery.Client(project="dw-avante-idds")
DATASET = "Financeiro"
TABLE_NAME = "conciliacao_diaria"

try:
    # ---------------------------
    # 1. Executar query no SQL Server
    # ---------------------------
    print("Extraindo dados...")
    query = """ 
with saldo_disponivel as (
select cast(datalancamento as date) as datalancamento  ,vbb.codctabcoint ,codctabco, valorlancamento as Saldo from AD_VIEWEXTBB vbb 
inner join TSICTA ct on ct.CODCTABCOINT = vbb.CODCTABCOINT 
where TEXTODESCRICAOHISTORICO = 'Saldo disponível'



) ,

saldo_pendente as ( 
select cast(datalancamento as date) as datalancamento  ,vbb.codctabcoint ,codctabco, valorlancamento as Saldo from AD_VIEWEXTBB vbb 
inner join TSICTA ct on ct.CODCTABCOINT = vbb.CODCTABCOINT 
where TEXTODESCRICAOHISTORICO = 'Tarifas Pendentes'
and datalancamento >='2025-12-01'
   
)
,

saldo_banco as ( 
select sd.datalancamento  ,sd.codctabcoint ,sd.codctabco,  
case when sd.Saldo = 0 and sp.Saldo <0 then isnull(sd.Saldo,0) +isnull(sp.Saldo,0) 
else sd.Saldo end Saldo
 from  saldo_disponivel sd left join 
saldo_pendente sp on sp.datalancamento = sd.datalancamento and sp.CODCTABCO = sd.CODCTABCO and sp.CODCTABCOINT = sd.CODCTABCOINT
and sd.CODCTABCOINT = 178


union all 

select cast(DATA_LNCAMENTO as date) as datalancamento,
asd.codctabcoint,codctabco,
Saldo
from AD_SALDOCONTAS asd 
inner join TSICTA ct on ct.CODCTABCOINT = asd.CODCTABCOINT 
where DATA_LNCAMENTO  >='2025-12-01'
)
,
 SUBQUERY AS (
    SELECT DISTINCT 
	replace(CT.CODCTABCO,'-','') as CODCTABCO, CT.CODCTABCOINT 
    FROM TGFMBC MB
    LEFT JOIN TSICTA CT ON CT.CODCTABCOINT = MB.CODCTABCOINT
	WHERE mb.dtlanc >='2024-01-01'

	
),
SUBQUERY_DOIS AS (
    SELECT SUM(BC.SALDOBCO) AS TOTAL_SALDO_BANCO, BC.REFERENCIA, replace(CT.CODCTABCO,'-','') AS CODCTABCO 
    FROM TGFSBC BC
    LEFT JOIN TSICTA CT ON CT.CODCTABCOINT = BC.CODCTABCOINT
    WHERE CT.CODCTABCO IN (SELECT CODCTABCO FROM SUBQUERY)
	AND BC.REFERENCIA >= '2024-01-01'
    GROUP BY BC.REFERENCIA, CT.CODCTABCO
), 




-- =====================================================
-- SALDO ACUMULADO DIÁRIO (igual TGFSBC mas por dia)
-- =====================================================
 MovimentosDiarios AS (
    -- Passo 1: Calcula o saldo de cada dia (igual a TGFSBC faz por mês)
    SELECT 
        CAST(MBC.DTLANC AS DATE) AS DATA,
        MBC.CODCTABCOINT AS CONTA,
        -- Soma todos os movimentos do dia (crédito positivo, débito negativo)
        SUM(CASE 
            WHEN MBC.RECDESP = 1 THEN MBC.VLRLANC  -- Crédito (aumenta saldo)
            ELSE -MBC.VLRLANC                       -- Débito (diminui saldo)
        END) AS SALDO_DIA
    FROM 
        TGFMBC MBC
     -- Sua conta específica
    GROUP BY 
        CAST(MBC.DTLANC AS DATE),
        MBC.CODCTABCOINT
),

SaldoAcumuladoDiario AS (
    -- Passo 2: Acumula os saldos dia a dia (igual TGFSBC acumula mês a mês)
    SELECT 
        MD.DATA,
        MD.CONTA,
        MD.SALDO_DIA,
        -- Soma acumulada de todos os dias até o atual
        SUM(MD.SALDO_DIA) OVER (
            PARTITION BY MD.CONTA
            ORDER BY MD.DATA
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS SALDO_ACUMULADO
    FROM 
        MovimentosDiarios MD
)

, base as (
    SELECT
        CONTA AS CODCTABCOINT,
        CT.CODCTABCO,
        CAST(DATA AS DATE) AS REFERENCIA,
        SALDO_DIA AS MOVIMENTO_DIA,
        SALDO_ACUMULADO AS SALDO_REAL
    FROM SaldoAcumuladoDiario sa
    INNER JOIN TSICTA ct 
        ON ct.CODCTABCOINT = sa.CONTA
   -- WHERE CONTA IN (178,179)
),

base_dois AS (
    SELECT 
        CODCTABCOINT,
        MAX(REFERENCIA) AS REFERENCIA,
        CAST(GETDATE() AS DATE) AS DATA_HOJE
    FROM base
    GROUP BY CODCTABCOINT
),

base_tres AS (
    SELECT 
        cta.CODCTABCO,
       max( b.REFERENCIA) as referencia,
        bd.DATA_HOJE,
        sum(b.SALDO_REAL) as SALDO_REAL
    FROM base b
    INNER JOIN base_dois bd 
	
        ON bd.CODCTABCOINT = b.CODCTABCOINT

       AND bd.REFERENCIA = b.REFERENCIA
	   inner join TSICTA cta on cta.CODCTABCOINT = b.CODCTABCOINT
	   GROUP BY cta.CODCTABCO,bd.DATA_HOJE
),

DateRange AS (
    -- Âncora
    SELECT REFERENCIA AS DateValue
    FROM base_tres

    UNION ALL

    -- Recursão
    SELECT DATEADD(DAY, 1, DateValue)
    FROM DateRange
    WHERE DateValue < CAST(GETDATE() AS DATE)
)
, base_SANKHYA as (
SELECT 
   
    dr.DateValue as referencia,
	codctabco,
   -- bt.CODCTABCOINT,
    bt.SALDO_REAL
FROM DateRange dr
CROSS JOIN base_tres bt
--inner join TSICTA cta on cta.CODCTABCOINT = bt.CODCTABCOINT
)
/*, base_sankhya as ( 
select referencia , 
SUM(saldo_real) as saldo_real,
codctabco from base_quatro
group by referencia,codctabco
*/
,ULTIMA_CONCILIACAO_BATENDO AS(
select ISNULL(bs.CODCTABCO,sb.codctabco) as Codctabco , 
ISNULL(bs.referencia, sb.datalancamento) as Data_Lançamento , 
SALDO_REAL as Saldo_Sankhya,
Saldo as Saldo_Banco ,
ISNULL(saldo ,0) - ISNULL(SALDO_REAL,0) as Saldo_Extrato_menos_Sankhya , 
case when ISNULL(saldo ,0) - ISNULL(SALDO_REAL,0) <=1 then 'Corretamente conciliada com o banco' 
when ISNULL(saldo ,0) - ISNULL(SALDO_REAL,0) >1 then 'Conciliações pendentes' else 'Saldo_sankhya maior que o saldo banco' end status 

from base_sankhya bs full outer join saldo_banco sb 
on sb.CODCTABCO = bs.CODCTABCO 
and CAST(sb.datalancamento AS date) =  CAST(bs.referencia AS date)
WHERE 
ISNULL(sb.datalancamento,bs.referencia) >='2025-12-01'
--filtro conta aqui
-- AND ISNULL(bs.CODCTABCO,sb.codctabco) = '1034367'            
AND(
ISNULL(saldo ,0) - ISNULL(SALDO_REAL,0) =0
OR ISNULL(saldo ,0) - ISNULL(SALDO_REAL,0) <=2
AND ISNULL(saldo ,0) - ISNULL(SALDO_REAL,0)>0
)
AND ISNULL(bs.referencia, sb.datalancamento) <=GETDATE()
)
,
ULTIMA_DATA_EXTRATO AS (
SELECT codctabco,MAX(datalancamento) DATALANCAMENTO FROM   AD_VIEWEXTBB VBB
inner join TSICTA ct on ct.CODCTABCOINT = vbb.CODCTABCOINT 
GROUP BY 
codctabco)


SELECT
DISTINCT
ude.CODCTABCO ,
ct.descricao as Nome_Conta,
UCB.DATALANCAMENTO AS Data_Ultima_Conciliacao_completa,


CASE 
    WHEN DATEDIFF(DAY, UCB.DATALANCAMENTO, UDE.DATALANCAMENTO) = 0 
        THEN CAST(DATEDIFF(DAY, UCB.DATALANCAMENTO, UDE.DATALANCAMENTO) AS VARCHAR)
    
    WHEN DATEDIFF(DAY, UCB.DATALANCAMENTO, UDE.DATALANCAMENTO) IS NULL 
        THEN 'mais que ' + CAST(DATEDIFF(MONTH, '2025-12-01', GETDATE()) AS VARCHAR) + ' meses.'
    
   ELSE 
    CASE 
        WHEN DATEDIFF(DAY, UCB.DATALANCAMENTO, UDE.DATALANCAMENTO) = -1 
             THEN '0'
        ELSE CAST(DATEDIFF(DAY, UCB.DATALANCAMENTO, UDE.DATALANCAMENTO) AS VARCHAR)
    END
END AS Dias_em_Atraso

from 
ULTIMA_DATA_EXTRATO ude left join ( 
select MAX(Data_Lançamento) as datalancamento,codctabco from 
ULTIMA_CONCILIACAO_BATENDO
group by codctabco
)ucb on ude.Codctabco =ucb.Codctabco
inner join TSICTA ct on ct.CODCTABCO = ude.CODCTABCO
WHERE (
    (
        -- Caso não seja NULL, verifica se é diferente de 0
        DATEDIFF(DAY, UCB.DATALANCAMENTO, UDE.DATALANCAMENTO) IS NOT NULL 
       -- AND CAST(DATEDIFF(DAY, UCB.DATALANCAMENTO, UDE.DATALANCAMENTO) AS VARCHAR) <> '0'
    )
    OR 
    -- Inclui os NULLs (que serão transformados na mensagem)
    DATEDIFF(DAY, UCB.DATALANCAMENTO, UDE.DATALANCAMENTO) IS NULL)
	and ct.DESCRICAO not like '%APL%'
	OPTION (MAXRECURSION 0);



---considerar o saldo pendente se o saldo disponivel for 0 
    

    """
    df = pd.read_sql(query, conn)
    print(f"Linhas extraídas: {len(df)}")

    # ---------------------------
    # 2. Calcular indicador diário
    # ---------------------------
    # 1️⃣ Criar flag para identificar textos "mais que"
    df['atraso_texto'] = df['Dias_em_Atraso'].astype(str).str.contains('mais que', case=False, na=False)

    # 2️⃣ Converter para número (textos viram NaN)
    df['Dias_em_Atraso'] = pd.to_numeric(df['Dias_em_Atraso'], errors='coerce')

    # 3️⃣ Calcular totais
    total_contas = df['CODCTABCO'].nunique()

    # 4️⃣ Considerar atraso:
    # - Dias_em_Atraso > 1
    # - OU atraso_texto == True
    em_atraso = df[
    (df['Dias_em_Atraso'] > 1) |
    (df['atraso_texto'])
    ]['CODCTABCO'].nunique()

    percentual_atraso = em_atraso / total_contas if total_contas > 0 else 0
    
    df_result = pd.DataFrame({
        'data_referencia': [date.today()],
        'contas_com_atraso': [em_atraso],
        'total_contas': [total_contas],
        'indicador': [percentual_atraso],
        'data_execucao': [datetime.now()]
    })

    # ---------------------------
    # 3. Enviar resultado para BigQuery
    # ---------------------------
    df_result.to_gbq(
        destination_table=f"{DATASET}.{TABLE_NAME}",
        project_id="dw-avante-idds",
        if_exists="append"
    )

    print("Indicador de conciliação diário registrado com sucesso no BigQuery.")

    # ---------------------------
    # 4. Log de SUCESSO no Sankhya
    # ---------------------------

    nome_script = "indicador_conciliacao.py"
    email = "francisco.alves@institutodds.org"
    chave_bq = "dw-avante-idds-625512a9cc6e.json"

    timestamp = datetime.now()
    chave_log = f"{timestamp:%Y-%m-%d %H:%M:%S} - indicador_conciliacao"

    cursor.execute("""
        INSERT INTO log_automacoes 
        (chave, nome_script, data, hora, status, email, link_log, mensagem_erro)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        chave_log,
        nome_script,
        date.today(),
        timestamp,
        "Script bem sucedido",
        email,
        None,
        None
    ))

    conn.commit()

# ---------------------------
# 5. Log de FALHA no Sankhya
# ---------------------------
except Exception as e:

    timestamp = datetime.now()
    chave_log = f"{timestamp:%Y-%m-%d %H:%M:%S} - indicador_conciliacao"

    cursor.execute("""
        INSERT INTO log_automacoes 
        (chave, nome_script, data, hora, status, email, link_log, mensagem_erro)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        chave_log,
        nome_script,
        date.today(),
        timestamp,
        "Script mal sucedido",
        email,
        None,
        str(e)
    ))

    conn.commit()

finally:
    cursor.close()
    conn.close()