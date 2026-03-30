# ETL de Conciliação Bancária

Esse projeto nasceu de um problema real: acompanhar diariamente o nível de conciliação entre o saldo do ERP (Sankhya) e o saldo do banco, no instituto em que trabalho como analista de dados.

Pode parecer simples, mas não é.

---

## Por que esse projeto?

Grande parte dos conteúdos de ETL mostram um fluxo muito limpo:

> extrair → transformar → carregar

Acontece que, na prática, não funciona assim.

No dia a dia, os dados não batem, não estão completos, seguem regras que ninguém documentou direit e às vezes simplesmente não fazem sentido

Esse ETL foi construído exatamente nesse cenário.

---

## O problema

A ideia é medir quantas contas estão com a conciliação em dia.

Só que pra chegar nisso, precisei lidar com coisas como:

- diferença entre saldo disponível e saldo real  
- tarifas pendentes que impactam o valor  
- movimentações que precisam ser acumuladas ao longo do tempo  
- dias sem informação  
- pequenas diferenças que precisam ser consideradas "ok"  

Ou seja: não é só comparar dois números.

---

## O que o ETL faz

### Extração (SQL Server)

A query já começa longe de ser simples:

- várias CTEs
- junções entre tabelas do Sankhya
- regras específicas pra calcular saldo

---

### Reconstrução de dados

Nem tudo vinha pronto.

Foi necessário:

- reconstruir o saldo diário a partir dos lançamentos  
- calcular saldo acumulado com `SUM OVER`  
- gerar datas manualmente (CTE recursiva)  

---

### Conciliação

Aqui entra a parte mais "negócio":

- comparar saldo do sistema vs banco  
- aplicar tolerância (diferenças pequenas são aceitáveis)  
- lidar com dados faltantes  

---

### Regras em Python

Depois do SQL, ainda tem tratamento em Python:

- lidar com valores tipo "mais que X meses"  
- identificar o que é atraso de fato  
- calcular o indicador final  

---

### Carga (BigQuery)

O resultado vai pro BigQuery, pra ser usado em análise e dashboard.

---

### Logging

O script registra no banco:

- se rodou com sucesso  
- se deu erro  

Porque sem isso, você não sabe quando o ETL quebrou.

---

## Indicador gerado

No final, o ETL produz:

- total de contas  
- contas em atraso  
- percentual de atraso  

---

## Tecnologias

- Python
- Pandas
- SQL Server
- BigQuery

---

## Como rodar

1. Criar um `.env` baseado no `.env.example`
2. Instalar dependências:

```bash
pip install -r requirements.txt
