import csv
import os
import json
from datetime import datetime

def convert_numeric(value):
    value = value.strip()
    if not value:
        return 0
    if 'K' in value:
        try:
            return float(value.replace('K', '')) * 1000
        except ValueError:
            return 0
    if 'M' in value:
        try:
            return float(value.replace('M', '')) * 1000000
        except ValueError:
            return 0
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return 0

def process_csv(input_file, output_file, categoria_forcada="infraestrutura"):
    processed_data = []
    tweet_ids_vistos = set()
    total_linhas = 0
    duplicados = 0

    # Carrega dados existentes, se houver
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as jsonfile:
            try:
                dados_existentes = json.load(jsonfile)
                for item in dados_existentes:
                    tweet_ids_vistos.add(item.get("tweet_id", 0))
            except Exception:
                dados_existentes = []
    else:
        dados_existentes = []

    # Processa CSV
    with open(input_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            total_linhas += 1
            tweet_id_str = row.get('Tweet ID', '').strip()
            if tweet_id_str.startswith("tweet_id:"):
                tweet_id_str = tweet_id_str[len("tweet_id:"):]
            try:
                tweet_id_num = int(tweet_id_str)
            except ValueError:
                continue
            if tweet_id_num in tweet_ids_vistos:
                duplicados += 1
                continue
            tweet_ids_vistos.add(tweet_id_num)

            timestamp_str = row.get('Timestamp', '').strip()
            try:
                timestamp_dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except Exception:
                timestamp_dt = None

            processed_record = {
                'tweet_id': tweet_id_num,
                'timestamp': timestamp_dt.isoformat() if timestamp_dt else None,
                'content': row.get('Content', '').strip(),
                'likes': convert_numeric(row.get('Likes', '')),
                'retweets': convert_numeric(row.get('Retweets', '')),
                'analytics': convert_numeric(row.get('Analytics', '')),
                'categoria': categoria_forcada
            }
            processed_data.append(processed_record)

    # Junta dados existentes com os novos e salva
    dados_finais = dados_existentes + processed_data
    with open(output_file, 'w', encoding='utf-8') as jsonfile:
        json.dump(dados_finais, jsonfile, indent=4, ensure_ascii=False)

    print(f"Processamento concluído!")
    print(f"Total de linhas no CSV: {total_linhas}")
    print(f"Novos tweets adicionados: {len(processed_data)}")
    print(f"Tweets ignorados por serem duplicados: {duplicados}")

if __name__ == "__main__":
    input_csv = '../tweets/infraestrutura_publica_tweets_1-109.csv'
    output_json = '../tweets/processados/brasilia_infraestrutura.json'
    process_csv(input_csv, output_json, categoria_forcada="infraestrutura")
