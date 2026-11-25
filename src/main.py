import os
import sys
import time
from datetime import datetime, timedelta

# permite rodar tanto "python src/main.py" quanto "python -m src.main"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _silenciar_connection_reset_windows():
    # Bug conhecido do asyncio no Windows (ProactorEventLoop): ao encerrar o
    # servidor temporário do Prefect, a conexão já fechada pelo outro lado gera
    # "ConnectionResetError: [WinError 10054]" no log. É inofensivo; aqui só
    # ignoramos esse erro específico no fechamento do transporte.
    if sys.platform != "win32":
        return
    from asyncio.proactor_events import _ProactorBasePipeTransport

    original = _ProactorBasePipeTransport._call_connection_lost

    def _call_connection_lost(self, exc):
        try:
            original(self, exc)
        except ConnectionResetError:
            if self._sock is not None:
                self._sock.close()
                self._sock = None

    _ProactorBasePipeTransport._call_connection_lost = _call_connection_lost


_silenciar_connection_reset_windows()

from prefect import flow

from src.extract import Extract
from src.transform import Transform
from src.load import Load

tabelas = {
    "cadastro": "cadastro_2026.csv",
    "monitoramento": "monitoramento.csv",
}


@flow(name="etl-ovitrampas", log_prints=True)
def etl_ovitrampas():
    extrator = Extract()
    transformador = Transform()
    carregamento = Load()

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


HORA_EXECUCAO = 6  # roda todo dia às 06:00


def segundos_ate_proxima_execucao():
    agora = datetime.now()
    proxima = agora.replace(hour=HORA_EXECUCAO, minute=0, second=0, microsecond=0)
    if proxima <= agora:
        proxima += timedelta(days=1)
    return (proxima - agora).total_seconds(), proxima


def rodar_agendado():
    # obs: o .serve(cron=...) do Prefect não agenda nada no servidor temporário
    # (ephemeral), por isso o agendamento diário é feito aqui mesmo em Python.
    while True:
        espera, proxima = segundos_ate_proxima_execucao()
        print(f"Próxima execução em {proxima:%d/%m/%Y %H:%M}. (Ctrl+C para parar)")
        time.sleep(espera)
        try:
            etl_ovitrampas()
        except Exception as erro:
            # se uma execução falhar, não derruba o agendamento
            print(f"Erro na execução do ETL: {erro}")


if __name__ == "__main__":
    if "--agendar" in sys.argv:
        # fica rodando e executa a pipeline todo dia às 06:00: python src/main.py --agendar
        rodar_agendado()
    else:
        # executa uma vez só: python src/main.py
        etl_ovitrampas()
