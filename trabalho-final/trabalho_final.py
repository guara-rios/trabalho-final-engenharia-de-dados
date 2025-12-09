# Trabalho Final de Engenharia de Dados
# Aluno: Guaraci Rios
# Código Principal

import os

from file_silver import bronze_to_silver
from file_gold import silver_to_gold

path_bronze = "data/bronze"
path_silver = "data/silver"
path_gold = "data/gold"

os.makedirs(path_bronze, exist_ok=True)
os.makedirs(path_silver, exist_ok=True)
os.makedirs(path_gold, exist_ok=True)

file_bronze = os.path.join(path_bronze, "amz_br.parquet")
file_silver = os.path.join(path_silver, "amz_br.parquet")

bronze_to_silver(file_bronze, file_silver)
silver_to_gold(file_silver, path_gold)

