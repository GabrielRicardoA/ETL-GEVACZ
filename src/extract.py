import os
import pandas as pd
from prefect import task
from prefect.cache_policies import NO_CACHE


class Extract():
    def __init__(self):
        self.pasta = os.path.dirname(os.path.abspath(__file__))  # pasta src/

    @task(cache_policy=NO_CACHE)
    def extract_csv(self, nome_arquivo, sep=","):
        caminho = os.path.join(self.pasta, nome_arquivo)
        return pd.read_csv(caminho, sep=sep, encoding="utf-8-sig", low_memory=False)