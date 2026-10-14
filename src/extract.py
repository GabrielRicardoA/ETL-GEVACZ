import os
import pandas as pd

class Extract():
    def __init__(self):
        self.pasta = os.path.dirname(os.path.abspath(__file__))  # pasta src/

    def extract_csv(self, nome_arquivo, sep=","):
        caminho = os.path.join(self.pasta, nome_arquivo)
        return pd.read_csv(caminho, sep=sep, encoding="utf-8-sig", low_memory=False)