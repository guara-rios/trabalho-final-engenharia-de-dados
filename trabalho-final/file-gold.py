# Trabalho Final de Engenharia de Dados
# Aluno: Guaraci Rios
# Código Gold

import os
import pandas as pd

path_silver = "data/silver"
path_gold = "data/gold"

os.makedirs(path_gold, exist_ok=True)

file_silver = os.path.join(path_silver, "amz_br.parquet")

def silver_to_gold(file_silver, path_gold):
    df = pd.read_parquet(file_silver)

    avaliacoes_por_categoria = df.sort_values(["categoryName", "stars"], ascending=[True, False])
    top3_por_categoria = avaliacoes_por_categoria.groupby("categoryName").head(3)
    top3_por_categoria = top3_por_categoria[["categoryName", "title", "stars"]]

    menores_avaliacoes_por_categoria = (
        df.sort_values(["categoryName", "stars"], ascending=[True, True])
        .groupby("categoryName").head(3)
    )[["categoryName", "title", "stars"]]

    top10_best_sellers = (
        df[df["isBestSeller"] == True]
        .sort_values("stars", ascending=False)
        .head(10)
    )[["categoryName", "title", "stars"]]

    menores_avaliacoes_best_sellers = (
        df[df["isBestSeller"] == True]
        .sort_values("stars", ascending=True)
        .head(10)
    )[["categoryName", "title", "stars"]]

    top3_por_categoria.to_parquet(os.path.join(path_gold, "top3_por_categoria.parquet"), index=False)
    menores_avaliacoes_por_categoria.to_parquet(os.path.join(path_gold, "menores_avaliacoes_por_categoria.parquet"), index=False)
    top10_best_sellers.to_parquet(os.path.join(path_gold, "top10_best_sellers.parquet"), index=False)
    menores_avaliacoes_best_sellers.to_parquet(os.path.join(path_gold, "menores_avaliacoes_best_sellers.parquet"), index=False)

    return{
        "top3_por_categoria": top3_por_categoria,
        "menores_avaliacoes_por_categoria": menores_avaliacoes_por_categoria,
        "top10_best_sellers": top10_best_sellers,
        "menores_avaliacoes_best_sellers": menores_avaliacoes_best_sellers

    }





