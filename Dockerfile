# Dockerfile para o pipeline ETL
FROM python:3.11-slim

WORKDIR /app

# Instalar dependências do sistema (necessário para psycopg2)
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código do projeto
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY dataset/ ./dataset/

# Definir variáveis de ambiente padrão
ENV PYTHONPATH=/app/src
ENV LOAD_TO_DB=true

# Comando padrão: executar o pipeline
CMD ["python", "scripts/run_pipeline.py"]
