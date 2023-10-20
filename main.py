import requests
import json
import os
import concurrent.futures
import os
from datetime import datetime

from dotenv import load_dotenv

from endpoints import get_endpoints


load_dotenv()
API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="


def search_endpoint(endpoint: str, numero_processo: str):
    numero_processo = os.getenv("NUMERO_PROCESSO")

    payload = json.dumps({"query": {"match": {"numeroProcesso": numero_processo}}})

    headers = {"Authorization": f"ApiKey {API_KEY}", "Content-Type": "application/json"}

    response = requests.request("POST", endpoint, headers=headers, data=payload)
    if response.status_code == 200:
        response_dict: dict = response.json()
        if response_dict["hits"]["total"]["value"] != 0:
            return endpoint, response_dict

        print(f"{endpoint:<70} Failed")


if __name__ == "__main__":
    os.system("cls||clear")
    numero_processo = os.getenv("NUMERO_PROCESSO")
    endpoints = get_endpoints()["URL"].values

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(
            executor.map(
                lambda endpoint: search_endpoint(endpoint, numero_processo), endpoints
            )
        )

    for result in results:
        if result:
            endpoint, response_dict = result
            print(f"\nProcesso encontrado no endpoint: {endpoint}\n")
            break

    os.system("cls||clear")

    hits = response_dict["hits"]["hits"][0]
    source = hits["_source"]
    movimentos = source["movimentos"]

    complementos_tabelados = [
        movimento for movimento in movimentos if "complementosTabelados" in movimento
    ]

    classe = source["classe"]["nome"]
    tribunal = source["tribunal"]
    assunto = source["assuntos"][0]["nome"]
    orgao_julgador = source["orgaoJulgador"]["nome"]

    # inicio = source["dataAjuizamento"]

    ultima_atualizacao = datetime.strptime(
        source["dataHoraUltimaAtualizacao"], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).strftime("%d/%m/%Y")
    ultima_atualizacao_movimento = datetime.strptime(
        movimentos[-1]["dataHora"], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).strftime("%d/%m/%Y")
    ultima_atualizacao_complementos = datetime.strptime(
        complementos_tabelados[-1]["dataHora"], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).strftime("%d/%m/%Y")

    print("---------------------------------------------------")
    print(tribunal)
    print(orgao_julgador)
    print(classe)
    print(assunto)
    print(f"Processo {numero_processo}")
    # print(f"Ajuizamento {inicio}")
    print("---------------------------------------------------")
    print(f"Ultima atualização ( @timestamp  ): {ultima_atualizacao}")

    print(f"Ultima atualização ( movimentação): {ultima_atualizacao_movimento}")
    print(f"Ultima atualização ( complementos): {ultima_atualizacao_complementos}")
    print("\n\n")

    movimentos
