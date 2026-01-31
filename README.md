# E-Commerce Data Pipeline ETL

Pipeline ETL end-to-end para dados de e-commerce, integrando múltiplas fontes de dados, limpando e transformando dados brutos, e carregando-os em um banco de dados relacional para suportar análises e tomada de decisões.

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