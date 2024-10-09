import boto3
from botocore.exceptions import NoCredentialsError
import requests
import pandas as pd

# Initialise la session S3 sans spécifier les identifiants, car ils seront fournis par IAM
s3 = boto3.client('s3')

# Nom du bucket et fichier à uploader
BUCKET_NAME = 'projetcryptoalex'


def check_file_exists_s3(bucket, file_name):
    try:
        s3.head_object(Bucket=bucket, Key=file_name)
        return True
    except Exception as e:
        return False

def download_parquet_from_s3(bucket, file_name):
    try:
        s3.download_file(bucket, file_name, f'/tmp/{file_name}')
        return pd.read_parquet(f'/tmp/{file_name}')
    except Exception as e:
        print(f"Erreur de téléchargement depuis S3 : {e}")
        return pd.DataFrame()

# Fonction pour uploader un fichier Parquet dans S3
def upload_parquet_to_s3(file_name, bucket):
    try:
        s3.upload_file(f'/tmp/{file_name}', bucket, file_name)
        print(f"Fichier {file_name} uploadé dans {bucket}")
    except FileNotFoundError:
        print(f"Le fichier {file_name} n'a pas été trouvé")
    except NoCredentialsError:
        print("Identifiants AWS manquants ou incorrects")



def retrieve_1mn(symbol):
    # URL de l'API Binance pour les bougies
    url = "https://api.binance.com/api/v3/klines"
    file_name = f"{symbol}.parquet"

    # Vérifie si le fichier existe dans S3
    if check_file_exists_s3(BUCKET_NAME, file_name):
        # Télécharge le fichier depuis S3
        df_init = download_parquet_from_s3(BUCKET_NAME, file_name)
    else:
        # Si le fichier n'existe pas, initialise un DataFrame vide
        df_init = pd.DataFrame()

    # Paramètres pour obtenir les données de la paire avec un intervalle de 1 minute
    params = {
        "symbol": symbol,
        "interval": "1m",  # Intervalle de 1 minute
        "limit": 1000      # Nombre maximum de bougies à récupérer (max 1000)
    }

    # Effectuer la requête GET à l'API Binance
    response = requests.get(url, params=params)

    # Vérifier si la requête est réussie
    if response.status_code == 200:
        # Charger les données en JSON
        data = response.json()
        # Transformer les données en DataFrame
        df = pd.DataFrame(data, columns=[
            'Open Time', 'Open', 'High', 'Low', 'Close', 'Volume', 
            'Close Time', 'Quote Asset Volume', 'Number of Trades', 
            'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore'
        ])

        # Convertir "Open Time" en datetime
        df['Open Time'] = pd.to_datetime(df['Open Time'], unit='ms')
        
        # Combiner avec le fichier existant
        df_combined = pd.concat([df_init, df]).drop_duplicates(subset=['Open Time']).reset_index(drop=True)

        # Sauvegarder le fichier combiné localement
        df_combined.to_parquet(f'/tmp/{file_name}')
        
        # Uploader le fichier combiné dans S3
        upload_parquet_to_s3(file_name, BUCKET_NAME)

    else:
        print(f"Erreur lors de la récupération des données : {response.status_code}")

    return df_combined


def lambda_handler(event, context):
    # Cette fonction est le point d'entrée de Lambda
    # Tu peux y placer le code que tu veux exécuter

    crypto_pairs = [
        "SOLUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "AXSUSDT",
        "ADAUSDT",
        "BTCUSDT",
        "AVAXUSDT",
        "LINKUSDT",
        "NEARUSDT",
        "RENDERUSDT",
        "POLUSDT",
        "AAVEUSDT",
        "ICPUSDT",
        "THETAUSDT",
        "DOTUSDT",
        "ALPACAUSDT",
        "SHIBUSDT",
        "XRPUSDT",
        "CHZUSDT",
        "FILUSDT",
        "ALGOUSDT",
        "ONEUSDT",
        "EGLDUSDT",
        "MANAUSDT",
        "VETUSDT",
        "AXLUSDT",
        "VTHOUSDT",
        "AIUSDT",
        "HOTUSDT",
        "ILVUSDT"
    ]

    for pair in crypto_pairs:
        retrieve_1mn(pair)

    return {
        'statusCode': 200,
        'body': 'Tâche exécutée avec succès'
    }