# Utilise l'image de base AWS Lambda Python 3.9
FROM public.ecr.aws/lambda/python:3.9

# Copie les fichiers dans le conteneur
COPY requirements.txt ./
COPY updates_crypto.py ./

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Commande à exécuter lorsque le conteneur démarre
CMD ["updates_crypto.lambda_handler"]
