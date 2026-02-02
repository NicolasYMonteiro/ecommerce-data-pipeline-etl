# E-Commerce Data Pipeline ETL

Pipeline ETL end-to-end para dados de e-commerce, integrando múltiplas fontes de dados, limpando e transformando dados brutos, e carregando-os em um banco de dados relacional para suportar análises e tomada de decisões.

## Objective:
To build a data pipeline for an e-commerce platform that consolidates orders, customers, products, payments, and reviews into an analytical model (star schema), ready for sales, logistics, and customer satisfaction analysis.

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Datasets](#datasets)

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
│   ├── database.yaml
│   └── pipeline.yaml
│
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
│   └── test_pipeline.py
│
├── scripts/
│   └── run_pipeline.py   
│
├── .gitignore                          # Arquivos ignorados pelo Git
├── requirements.txt                    # Dependências do projeto
├── README.md                           # Este arquivo
```

### Descrição das Pastas Principais
- **`src/utils/`**: Utilitários compartilhados como logging, configurações e funções auxiliares.
- **`src/pipeline/`**: Orquestração principal do pipeline ETL.
- **`config/`**: Arquivos de configuração em formato YAML para facilitar manutenção.
- **`tests/`**: Testes organizados por módulo para garantir qualidade do código.
- **`scripts/`**: Scripts auxiliares para setup e execução.

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

**Características**: O processo de carregamento é totalmente automatizado, idempotente (pode ser executado múltiplas vezes sem duplicar dados) e otimizado para consultas analíticas. A configuração é feita via variáveis de ambiente (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT).

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