import os
import certifi
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from prefect import task
from prefect.cache_policies import NO_CACHE

load_dotenv()


class Transform():
    def __init__(self):
        self.client = MongoClient(os.getenv("MONGODB_uri"), tlsCAFile=certifi.where())
        self.db_name = "OVITRAMPAS"

    def ler_bronze(self, collection_name):
        docs = self.client[self.db_name][collection_name].find({}, {"_id": 0})
        return pd.DataFrame(list(docs))

    def para_texto(self, df, colunas):
        for col in colunas:
            df[col] = df[col].astype("string").str.strip()
        return df

    def para_inteiro(self, df, colunas):
        for col in colunas:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        return df

    def para_data(self, df, colunas):
        # datas no formato AAAA-MM-DD; o que estiver errado vira vazio (NaT)
        for col in colunas:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")
        return df

    @task(cache_policy=NO_CACHE)
    def transform_to_silver(self, tabela):
        df = self.ler_bronze(tabela)

        # colunas que existem nas duas tabelas
        df = self.para_texto(df, ["arquivo", "aba", "bairro", "id_ovt", "agente_matricula"])
        df["bairro"] = df["bairro"].str.upper()
        df["id_ovt"] = df["id_ovt"].str.upper()
        df = self.para_inteiro(df, ["linha", "ano", "distrito", "qt"])

        if tabela == "cadastro":
            df = self.para_texto(df, ["endereco"])
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
            df = self.para_data(df, ["data_cadastro"])

        elif tabela == "monitoramento":
            df = self.para_texto(df, ["bairro_area", "tecnico_lab", "observacao"])
            df["bairro_area"] = df["bairro_area"].str.upper()
            df = self.para_inteiro(df, ["coluna_ciclo", "ciclo", "numero_ovos"])
            df = self.para_data(df, ["data_campo", "data_entrega_apoio", "data_entrega_lab",
                                     "data_leitura", "data_digitacao", "data_envio_ds"])

        df = df.drop_duplicates()
        print(f"dados de '{tabela}' transformados em silver!")
        return df
