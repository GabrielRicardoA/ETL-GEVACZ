from src.extract import Extract
from src.transform import Transform
from src.load import Load

extrator = Extract()
transformador = Transform()
carregamento = Load()

tabelas = {
    "cadastro": "cadastro_2026.csv",
    "monitoramento": "monitoramento.csv",
}

for nome, arquivo in tabelas.items():
    print(f"--- ETL: {nome} ---")

    # Extract
    dados_bronze = extrator.extract_csv(arquivo)

    # Load bronze (Mongo)
    carregamento.load_mongo(dados_bronze, "OVITRAMPAS", nome)

    # Transform (lê do Mongo e devolve silver)
    dados_silver = transformador.transform_to_silver(nome)

    # Load silver (Neon)
    carregamento.load_neon(dados_silver, nome)

print("ETL finalizado!")
