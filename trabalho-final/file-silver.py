# Trabalho Final de Engenharia de Dados
# Aluno: Guaraci Rios
# Código Silver

import os
import pandas as pd

path_bronze= "data/bronze"
path_silver= "data/silver"

os.makedirs(path_silver, exist_ok=True)

file_bronze = os.path.join(path_bronze, "amz_br.parquet")

# asin
# title
# imgUrl
# productURL
# stars
# reviews
# price
# listPrice
# categoryName
# isBestSeller

def bronze_to_silver(file_bronze, file_silver):
    df = pd.read_parquet(file_bronze)

    df["asin"] = df["asin"].str.strip().str.title()
    df["title"] = df["title"].str.strip().str.title()
    df["imgUrl"] = df["imgUrl"].str.strip()
    df["productURL"] = df["productURL"].str.strip()
    df["stars"] = pd.to_numeric(df["stars"], errors="coerce").fillna(0)
    df["reviews"] = pd.to_numeric(df["reviews"], errors="coerce").fillna(0)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["listPrice"] = pd.to_numeric(df["listPrice"], errors="coerce").fillna(0)
    df["categoryName"] = df["categoryName"].str.strip().str.title()
    df["isBestSeller"] = df["isBestSeller"].astype(bool)

    df = df.groupby("asin", as_index=False).agg({
        "title": "first",
        "imgUrl": "first",
        "productURL": "first",
        "stars": "mean",
        "reviews": "sum",
        "price": "first",
        "listPrice": "first",
        "categoryName": "first",
        "isBestSeller": "first",

    })

    df.to_parquet(file_silver, index=False)
    return df

file_silver = os.path.join(path_silver, "amz_br.parquet")

bronze_to_silver(file_bronze, file_silver)

