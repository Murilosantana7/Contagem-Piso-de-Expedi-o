import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import requests
import base64
import json

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1hoXYiyuArtbd2pxMECteTFSE75LdgvA2Vlb6gPpGJ-g'
NOME_ABA = 'Contagem'
INTERVALO = 'C:H'
WEBHOOK_URL = "https://openapi.seatalk.io/webhook/group/5KZq9RrWR5eEbMCzBoapOw"

def autenticar_google():
    cred_base64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    if not cred_base64:
        raise ValueError("A variável de ambiente GOOGLE_CREDENTIALS_BASE64 não foi definida. Verifique seu workflow e secrets.")

    try:
        cred_json = base64.b64decode(cred_base64)
        cred_dict = json.loads(cred_json)

        # DEBUG opcional: mostre os campos presentes nas credenciais
        print("🔍 DEBUG: Campos encontrados na credencial:")
        print(list(cred_dict.keys()))

        creds = Credentials.from_service_account_info(cred_dict, scopes=SCOPES)
        return creds

    except Exception as e:
        raise RuntimeError(f"Erro ao processar as credenciais: {e}")

def obter_totais_por_fanout(spreadsheet_id, nome_aba, intervalo):
    try:
        creds = autenticar_google()
        client = gspread.authorize(creds)
        planilha = client.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(nome_aba)
    except Exception as e:
        return f"Erro ao conectar com a planilha: {e}"

    try:
        dados = aba.get(intervalo)
    except gspread.exceptions.APIError as e:
        return f"Erro na API do Google Sheets: {e}"
    except Exception as e:
        return f"Erro ao ler os dados da planilha: {e}"

    header_row_index = -1
    for i, row in enumerate(dados):
        if row and 'FANOUT' in [cell.strip().upper() for cell in row]:
            header_row_index = i
            break
    if header_row_index == -1:
        return "Não foi possível encontrar a linha do cabeçalho 'FANOUT'."

    headers = [h.strip() for h in dados[header_row_index]]
    data = dados[header_row_index + 1:]
    if not data:
        return "Nenhum dado encontrado após o cabeçalho."

    df = pd.DataFrame(data, columns=headers)
    df.columns = [col.strip() for col in df.columns]

    colunas_a_somar = ['PALLET/SCUTTLE', 'GAIOLA', 'SACA']
    for col in colunas_a_somar:
        if col not in df.columns:
            return f"A coluna '{col}' não foi encontrada na aba."

    for col in colunas_a_somar:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    totais_por_fanout = df.groupby('FANOUT', sort=False)[colunas_a_somar].sum().reset_index()
    totais_por_fanout = totais_por_fanout[(totais_por_fanout[colunas_a_somar] != 0).any(axis=1)]
    if totais_por_fanout.empty:
        return "Nenhum dado encontrado com valores maiores que zero."

    totais_por_fanout['FANOUT'] = totais_por_fanout['FANOUT'].str.strip()

    larguras = {'FANOUT': max(len('FANOUT'), totais_por_fanout['FANOUT'].str.len().max())}
    for col in colunas_a_somar:
        larguras[col] = max(len(col), totais_por_fanout[col].astype(int).astype(str).str.len().max())

    mensagens_list = []
    header_parts = ["FANOUT".ljust(larguras['FANOUT'])] + [col.center(larguras[col]) for col in colunas_a_somar]
    mensagens_list.append(" | ".join(header_parts))
    separator_parts = ["-" * larguras['FANOUT']] + ["-" * larguras[col] for col in colunas_a_somar]
    mensagens_list.append("-+-".join(separator_parts))

    for _, row in totais_por_fanout.iterrows():
        linha_parts = [str(row['FANOUT']).ljust(larguras['FANOUT'])] + [str(int(row[col])).center(larguras[col]) for col in colunas_a_somar]
        mensagens_list.append(" | ".join(linha_parts))

    return "\n".join(mensagens_list)

def enviar_webhook(mensagem):
    try:
        mensagem_formatada = "```\n" + mensagem + "\n```"
        payload = {
            "tag": "text",
            "text": {
                "format": 1,
                "content": mensagem_formatada
            }
        }
        response = requests.post(url=WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print("Mensagem enviada com sucesso.")
    except requests.exceptions.RequestException as err:
        print(f"Erro ao enviar mensagem para o webhook: {err}")

def main():
    mensagem_unica = obter_totais_por_fanout(SPREADSHEET_ID, NOME_ABA, INTERVALO)
    print("Mensagem a enviar:\n", mensagem_unica)
    if not mensagem_unica.startswith("Erro") and not mensagem_unica.startswith("Nenhum"):
        mensagem_final = "Segue o piso da expedição\n\n" + mensagem_unica
        enviar_webhook(mensagem_final)
    else:
        print("Mensagem não enviada pois houve erro ou não há dados válidos.")

if __name__ == "__main__":
    main()
