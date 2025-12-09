# Trabalho Final de Engenharia de Dados
# Aluno: Guaraci Rios
# Código Bronze

import os
import pandas as pd

path_bronze = "data/bronze"

os.makedirs(path_bronze, exist_ok=True)

def extract_to_bronze():
    df = pd.read_csv("data/bronze/amz_br_total_products_data_processed.csv")

    df.to_parquet("data/bronze/amz_br.parquet", index=False)

    return df






