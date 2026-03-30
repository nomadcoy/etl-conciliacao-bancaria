## ETL de Conciliação Bancária

Este projeto surgiu de um problema real no meu dia a dia como analista de dados: acompanhar diariamente o nível de conciliação entre o saldo registrado no ERP (Sankhya) e o saldo do banco no instituto em que trabalho. À primeira vista, pode parecer simples, mas na prática os dados raramente cooperam.

A maioria dos conteúdos sobre ETL mostra o fluxo como algo limpo e linear: extrair, transformar, carregar. No mundo real, os dados chegam incompletos, inconsistentes, com regras não documentadas ou até mesmo sem sentido aparente. Foi nesse cenário que este ETL foi construído, para lidar com a realidade de dados imperfeitos e processos complexos.

O objetivo principal é medir quantas contas estão conciliadas corretamente. Mas chegar a esse indicador exigiu lidar com nuances que vão além de uma simples comparação de números: diferenças entre saldo disponível e saldo real, tarifas pendentes que afetam o valor, movimentações acumuladas ao longo do tempo, dias sem informações, e pequenas discrepâncias que devem ser consideradas aceitáveis.

Na etapa de extração, uma query relativamente complexa acessa o SQL Server, utilizando várias CTEs e junções entre tabelas do Sankhya, aplicando regras específicas para calcular o saldo de cada conta. Nem tudo vem pronto: é necessário reconstruir o saldo diário a partir dos lançamentos, calcular o saldo acumulado com SUM OVER e gerar datas manualmente através de CTEs recursivas.

Depois, na fase de conciliação, o saldo do sistema é comparado com o saldo do banco, aplicando tolerâncias para pequenas diferenças e lidando com dados faltantes. Em Python, o script ainda trata valores especiais, identifica quais contas estão realmente em atraso e calcula o indicador final.

O resultado vai para o BigQuery, pronto para ser utilizado em análises e dashboards. Paralelamente, o script registra logs no banco, indicando se a execução foi bem-sucedida ou se houve algum erro, garantindo rastreabilidade e monitoramento do ETL.

Ao final, o ETL produz um indicador consolidado com o total de contas, o número de contas em atraso e o percentual de atraso, oferecendo uma visão diária confiável sobre a conciliação bancária.

O projeto utiliza Python, Pandas para manipulação de dados, SQL Server para extração e BigQuery para armazenamento e análise.

Para rodar, basta criar um arquivo .env baseado no .env.example, instalar as dependências com:

pip install -r requirements.txt

e executar o script principal.

Se quiser, posso fazer uma versão ainda mais enxuta e “blog-like”, que conte a história de forma mais narrativa, sem listas e subtítulos, para ficar bem leve de ler no GitHub. Quer que eu faça essa versão também?
