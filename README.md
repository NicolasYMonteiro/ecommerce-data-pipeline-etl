# E-Commerce Data Pipeline ETL

Pipeline ETL end-to-end para dados de e-commerce, integrando múltiplas fontes de dados, limpando e transformando dados brutos, e carregando-os em um banco de dados relacional para suportar análises e tomada de decisões.

## Objective:
To build a data pipeline for an e-commerce platform that consolidates orders, customers, products, payments, and reviews into an analytical model (star schema), ready for sales, logistics, and customer satisfaction analysis.

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Módulos do Pipeline](#-módulos-do-pipeline)
- [Sistema de Configuração](#srcutilsconfigpy---sistema-de-configuração)
- [Sistema de Logging](#srcutilsloggerpy---sistema-de-logging)
- [Testes](#-testes)
- [Datasets](#-datasets)

## 🎯 Visão Geral

Este projeto implementa um pipeline ETL completo para processar dados de e-commerce da Olist, incluindo:

- **Extract (Extração)**: Leitura de múltiplos arquivos CSV
- **Transform (Transformação)**: Limpeza, validação e enriquecimento de dados
- **Load (Carregamento)**: Inserção dos dados processados em banco de dados relacional

## 📁 Estrutura do Projeto

Nota de Arquitetura
A estrutura foi pensada para equilibrar simplicidade e boas práticas.
Evitei granularidade excessiva em módulos para não introduzir complexidade artificial, mantendo apenas abstrações que seriam comuns em pipelines de produção de pequeno e médio porte.
```
ecommerce-data-pipeline-etl/
│
├── dataset/                          # Dados brutos (CSV)
│   ├── olist_customers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   └── product_category_name_translation.csv
│
├── src/
│   ├── extract.py                     # Leitura e validação básica dos CSVs
│   ├── transform.py                   # Limpeza, joins e regras de negócio
│   ├── load.py                        # Escrita no banco de dados
│   ├── pipeline.py                    # Orquestração do ETL
│   │
│   └── utils/
│       ├── config.py                  # Leitura de configs (YAML/env)
│       └── logger.py                  # Logging centralizado
│
├── notebook/
│   ├── 01_exploratory_analysis.ipynb
│   
├── config/
│   ├── pipeline.yaml              # Configurações do pipeline
│   └── dataset.yaml                # Mapeamento de datasets
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Fixtures compartilhadas
│   ├── test_config.py              # Testes de configuração
│   ├── test_extract.py             # Testes de extração
│   ├── test_transform.py          # Testes de transformação
│   └── test_pipeline.py            # Testes de pipeline
│
├── logs/                           # Diretório de logs (gerado automaticamente)
│
├── scripts/
│   └── run_pipeline.py   
│
├── .gitignore                          # Arquivos ignorados pelo Git
├── .env                                 # Variáveis de ambiente (não versionado)
├── requirements.txt                    # Dependências do projeto
├── pytest.ini                          # Configuração do pytest
├── docker-compose.yml                  # Orquestração Docker
├── Dockerfile                          # Imagem Docker do ETL
└── README.md                           # Este arquivo
```

### Descrição das Pastas Principais
- **`src/utils/`**: Utilitários compartilhados (config, logger)
- **`config/`**: Arquivos de configuração em formato YAML (pipeline, datasets)
- **`tests/`**: Testes unitários organizados por módulo
- **`scripts/`**: Scripts de execução do pipeline
- **`logs/`**: Arquivos de log gerados automaticamente

## 🔧 Módulos do Pipeline

### `src/extract.py` - Módulo de Extração

O módulo de extração é responsável pela leitura e validação inicial dos arquivos CSV. Implementa as seguintes funcionalidades:

- **Leitura de CSVs**: Extração de todos os 9 datasets do diretório `dataset/raw/`
- **Validação de Schema**: Verificação automática das colunas esperadas em cada dataset
- **Tipagem Inicial**: Aplicação de tipos de dados apropriados (string, Int64, Float64, float64) para otimização de memória e validação
- **Logging de Volume**: Registro detalhado de métricas por dataset:
  - Número de linhas e colunas
  - Uso de memória (MB)
  - Total de valores faltantes
  - Tempo de execução

**Resultados da Extração**: O módulo retorna um dicionário contendo todos os DataFrames validados, prontos para a etapa de transformação. Cada dataset é validado individualmente, garantindo que apenas dados com schema correto prossigam no pipeline. Em caso de falha na validação, o módulo registra erros detalhados no log e continua processando os demais datasets.

### `src/transform.py` - Módulo de Transformação

O módulo de transformação é responsável pela limpeza, padronização, enriquecimento e criação de métricas derivadas. Implementa as seguintes funcionalidades:

- **Padronização de Colunas**: Conversão para lowercase e snake_case para consistência
- **Tratamento de Valores Faltantes**: Regras específicas por dataset (strings → 'unknown', numéricos mantidos como NaN)
- **Conversão de Datas**: Transformação de strings para datetime com validação
- **Enriquecimento de Dados**: Merge com tradução de categorias de produtos, identificação de clientes recorrentes
- **Criação de Métricas**: 
  - Valor total do pedido (soma de itens + frete) - regra de negócio principal
  - Tempo de entrega em dias e atraso em relação ao estimado
  - Validação de coordenadas geográficas do Brasil
- **Tabela Fato Consolidada**: Criação de tabela fato com joins de todos os datasets e agregações (pagamentos, avaliações, produtos, vendedores)

**Regras de Negócio Documentadas**: Todas as regras de transformação estão documentadas no código, incluindo a regra principal de que o valor total do pedido é calculado a partir da soma dos itens + frete, pois o dataset de pedidos não traz esse campo consolidado.

### `src/load.py` - Módulo de Carregamento

O módulo de carregamento é responsável pela inserção dos dados no PostgreSQL, implementando uma arquitetura em duas camadas:

- **Staging Layer**: Carregamento dos dados brutos com transformações mínimas
  - Metadados de rastreabilidade: `source` e `load_timestamp` em todas as tabelas
  - Idempotência: DELETE + INSERT por fonte, permitindo reprocessamento seguro
  - Preservação dos dados originais para auditoria

- **Analytics Layer (Star Schema)**: Modelo estrela otimizado para consultas analíticas
  - **Dimensões**: `dim_time`, `dim_customers`, `dim_products`, `dim_sellers`, `dim_geography`
  - **Tabela Fato**: `fact_orders` com métricas consolidadas e foreign keys para todas as dimensões
  - **Idempotência**: UPSERT (ON CONFLICT) em todas as tabelas para garantir reprocessamento seguro
  - **Otimizações**: Índices em foreign keys e colunas de filtro para performance

**Características**: O processo de carregamento é totalmente automatizado, idempotente (pode ser executado múltiplas vezes sem duplicar dados) e otimizado para consultas analíticas. A configuração é feita via arquivos YAML e variáveis de ambiente.

### `src/utils/config.py` - Sistema de Configuração

O módulo de configuração centraliza todas as configurações do pipeline, permitindo alterar comportamento sem modificar código:

- **Arquivos de Configuração**:
  - `config/pipeline.yaml`: Configurações do pipeline (paths, database, logging, behavior)
  - `config/dataset.yaml`: Mapeamento de datasets para arquivos CSV
  - `.env`: Variáveis de ambiente (opcional, sobrescreve YAML)

- **Prioridade de Configuração**: Variáveis de ambiente > YAML > Valores padrão

- **Funcionalidades**:
  - Singleton pattern para acesso global
  - Suporte a valores aninhados: `config.get('database.host')`
  - Paths como objetos `Path`: `config.data_dir`, `config.logs_dir`
  - Configurações de banco: `config.database_config`
  - Configurações de pipeline: `config.pipeline_config`

- **Exemplo de Uso**:
  ```python
  from src.utils.config import config
  
  # Acessar configurações
  db_host = config.get('database.host')
  data_dir = config.data_dir
  batch_size = config.get('pipeline.batch_size')
  ```

### `src/utils/logger.py` - Sistema de Logging

O módulo de logging fornece observabilidade completa do pipeline:

- **Funcionalidades**:
  - Logging simultâneo em console e arquivo
  - Arquivos com timestamp: `pipeline_20240202.log`
  - Níveis configuráveis (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  - Formatação padronizada com timestamp, módulo e nível
  - Logs salvos em `logs/` (configurável)

- **Integração**: Todos os módulos (`extract`, `transform`, `load`, `pipeline`) utilizam o logger centralizado

- **Exemplo de Uso**:
  ```python
  from src.utils.logger import get_logger
  
  logger = get_logger(__name__)
  logger.info("Processando dados...")
  logger.error("Erro ao processar", exc_info=True)
  ```

## 🧪 Testes

O projeto inclui uma suíte de testes para garantir qualidade e confiabilidade do código:

- **Framework**: pytest com cobertura de código
- **Estrutura**: Testes organizados por módulo em `tests/`

### Executando Testes

```bash
# Executar todos os testes
pytest

# Com cobertura de código
pytest --cov=src --cov-report=html

# Teste específico
pytest tests/test_extract.py -v

# Com output detalhado
pytest -v
```

### Cobertura de Testes

- **test_config.py**: Testes do sistema de configuração (singleton, paths, database, env vars)
- **test_extract.py**: Testes de extração (validação de schema, tipos, arquivos)
- **test_transform.py**: Testes de transformação (padronização, tratamento de nulos, métricas, tabela fato)
- **test_pipeline.py**: Testes de orquestração do pipeline

### Fixtures Compartilhadas

O arquivo `tests/conftest.py` fornece fixtures reutilizáveis:
- `sample_orders_df`: DataFrame de exemplo para pedidos
- `sample_order_items_df`: DataFrame de exemplo para itens
- `temp_data_dir`: Diretório temporário para dados de teste

## 📊 Datasets

O projeto utiliza os seguintes datasets da Olist:

### `olist_customers_dataset.csv`
- **Descrição**: Contém informações sobre clientes e suas localizações.
- **Observações**: 
  - Cada pedido recebe um `customer_id` único.
  - O `customer_unique_id` permite identificar clientes que fizeram compras repetidas.

### `olist_geolocation_dataset.csv`
- **Descrição**: Informações sobre CEPs brasileiros e suas coordenadas geográficas (latitude e longitude).

### `olist_order_items_dataset.csv`
- **Descrição**: Informações sobre os itens comprados em cada pedido.

### `olist_order_payments_dataset.csv`
- **Descrição**: Informações sobre as opções de pagamento dos pedidos.

### `olist_order_reviews_dataset.csv`
- **Descrição**: Informações sobre as avaliações feitas pelos clientes.

### `olist_orders_dataset.csv`
- **Descrição**: **Dataset principal**. A partir de cada pedido, é possível encontrar todas as outras informações relacionadas.

### `olist_products_dataset.csv`
- **Descrição**: Informações sobre os produtos vendidos pela Olist.

### `olist_sellers_dataset.csv`
- **Descrição**: Informações sobre os vendedores que processaram pedidos na Olist.

### `product_category_name_translation.csv`
- **Descrição**: Tradução dos nomes das categorias de produtos para inglês.

## 🐳 Estrutura Docker

O projeto utiliza Docker e Docker Compose para facilitar a execução e garantir consistência entre ambientes.

### Arquitetura Docker

O `docker-compose.yml` define dois serviços:

1. **postgres**: Banco de dados PostgreSQL 15
   - Porta: `5432` (configurável via `DB_PORT`)
   - Volume persistente: `postgres_data` para dados do banco
   - Healthcheck: Verifica se o banco está pronto antes de iniciar o ETL
   - Variáveis de ambiente: `DB_NAME`, `DB_USER`, `DB_PASSWORD`

2. **etl**: Container do pipeline ETL
   - Base: Python 3.11-slim
   - Dependências: Instaladas via `requirements.txt`
   - Volumes montados:
     - `./dataset` → `/app/dataset` (dados CSV)
     - `./logs` → `/app/logs` (arquivos de log)
   - Dependência: Aguarda o PostgreSQL estar saudável antes de iniciar
   - Comando: Executa `scripts/run_pipeline.py` automaticamente

### Dockerfile

O `Dockerfile` do ETL:
- Instala dependências do sistema (gcc, postgresql-client)
- Instala dependências Python do `requirements.txt`
- Copia código fonte, scripts e dados
- Define `PYTHONPATH` e variáveis de ambiente
- Executa o pipeline automaticamente ao iniciar

### Rede Docker

- Rede isolada `etl_network` conecta os serviços
- O container ETL acessa o PostgreSQL pelo hostname `postgres`

## 🚀 Executando o Pipeline

### Opção 1: Execução com Docker (Recomendado)

A forma mais simples de executar o pipeline completo:

```bash
# 1. Criar arquivo .env (opcional, se quiser sobrescrever defaults)
DB_HOST=localhost
DB_NAME=ecommerce_olist
DB_USER=postgres
DB_PASSWORD=postgres
DB_PORT=5432
LOAD_TO_DB=true

# 2. Executar com Docker Compose
docker compose up -d --build

# 3. Executar etl com logs
docker compose logs -f etl

# 4. Parar os containers
docker compose down

# 5. Parar e remover volumes (limpar dados do banco)
docker compose down -v
```

**O que acontece:**
1. Docker Compose inicia o PostgreSQL
2. Aguarda o banco estar pronto (healthcheck)
3. Constrói a imagem do ETL
4. Inicia o container ETL que executa o pipeline automaticamente
5. Dados são carregados no banco PostgreSQL

### Opção 2: Execução Local (Sem Docker)

Para executar localmente, você precisa ter Python e PostgreSQL instalados:

```bash
# 1. Criar ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate  # Windows

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar banco de dados (criar .env ou editar config/pipeline.yaml)
DB_HOST=localhost
DB_NAME=ecommerce_olist
DB_USER=postgres
DB_PASSWORD=postgres
DB_PORT=5432
LOAD_TO_DB=true

# 4. Executar pipeline
python scripts/run_pipeline.py

# Ou apenas Extract + Transform (sem carregar no banco)
# Edite config/pipeline.yaml: load_to_db: false
python scripts/run_pipeline.py
```

### Opção 3: Executar Apenas Testes

```bash
# Instalar dependências de teste (já incluídas no requirements.txt)
pip install -r requirements.txt

# Executar todos os testes
pytest

# Com cobertura
pytest --cov=src --cov-report=html

# Teste específico
pytest tests/test_extract.py -v
```