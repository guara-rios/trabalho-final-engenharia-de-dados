# Projeto de ETL da Amazon Brasil

O projeto realiza a extração, transformação e carregamento dos dados da Amazon Brasil e processa as camadas bronze, silver e gold.
Dataset retirado do kaggle: https://www.kaggle.com/datasets/asaniczka/amazon-brazil-products-2023-1-3m-products?resource=download

## Estrutura das pastas

|-----trabalho-final/

   |-----data/

       |-----bronze/Dados brutos
       |-----silver/Dados limpos
       |-----gold/Dados finais

## Scripts

### 1. file_bronze
Não trata nenhum dado.
Converte o dataset do formato csv para parquet.

### 2. file_silver
Tranforma os dados da camada bronze para a camada silver:
-Padroniza as colunas (Retira espaços iniciais e final das strings, coloca em formato de título, faz conversão numérica e booleana)
-Agrupamento pela coluna "asin" e agregações(média de avaliações, soma de reviews)
-Salva o resultado em "data/silver/amz_br.parquet"

### 3. file_gold
Transforma os dados da camada silver para a camada gold:
-Gera as seguintes análises:
-Top 3 produtos melhor avaliados por categoria
-Top 3 produtos pior avaliados por categoria
-Top 10 Best Sellers melhor avaliados
-Top 10 Best Sellers pior avaliados

### trabalho_final
Executa toda a pipeline:
-Usa a função que transforma os dados da camada bronze para silver.
-Usa a função que transforma os dados da camada silver para gold.
-Garante que todas as pastas existam antes de rodar o código.
-Salva os arquivos em suas pastas.

