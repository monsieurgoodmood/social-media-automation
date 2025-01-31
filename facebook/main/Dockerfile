# Dockerfile

# Utilisation de l'image Python slim pour un environnement léger
FROM python:3.9-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier tout le contenu de votre projet dans le conteneur
COPY . .

# Installer les dépendances requises à partir du fichier requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Installer Gunicorn pour servir l'application Flask en production
RUN pip install gunicorn

# Exposer le port 8080 (port par défaut utilisé par Cloud Run)
EXPOSE 8080

# Démarrer l'application avec Gunicorn, lier l'application sur l'adresse 0.0.0.0 et le port 8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]