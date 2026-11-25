import io
import os
import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine
from prefect import task
from prefect.cache_policies import NO_CACHE

load_dotenv()


class Load():
    def __init__(self):
        self.client = MongoClient(os.getenv("MONGODB_uri"), tlsCAFile=certifi.where())
        self.engine = create_engine(os.getenv("DATABASE_URL"))

    @task(cache_policy=NO_CACHE)
    def load_mongo(self, df, db_name, collection_name):
        # troca os vazios (NaN) por None, que o Mongo entende
        df = df.astype(object).where(df.notna(), None)
        registros = df.to_dict("records")

        colecao = self.client[db_name][collection_name]
        colecao.delete_many({})  # apaga o que tinha para não duplicar
        colecao.insert_many(registros)
        print(f"{len(registros)} registros inseridos na coleção '{collection_name}'!")

    @task(cache_policy=NO_CACHE)
    def load_neon(self, df, nome_tabela):
        print(f"salvando '{nome_tabela}' no Neon...")

        # 1) cria a tabela vazia, só com as colunas e os tipos
        df.head(0).to_sql(nome_tabela, self.engine, if_exists="replace", index=False)

        # 2) transforma os dados em texto CSV (vazios viram NULL no banco)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # 3) COPY manda tudo de uma vez (bem mais rápido que um INSERT por linha)
        conexao = self.engine.raw_connection()
        try:
            cursor = conexao.cursor()
            with cursor.copy(f'COPY "{nome_tabela}" FROM STDIN WITH (FORMAT CSV)') as copy:
                copy.write(buffer.getvalue())
            conexao.commit()
        finally:
            conexao.close()

        print(f"{len(df)} registros salvos no Neon na tabela '{nome_tabela}'!")
