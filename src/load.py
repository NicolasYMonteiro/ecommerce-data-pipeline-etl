"""
Módulo de Carregamento de Dados
Responsável pelo carregamento em PostgreSQL (staging e star schema)
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

logger = logging.getLogger(__name__)


def convert_pandas_value(val):
    """
    Converte valores do pandas para tipos Python nativos compatíveis com PostgreSQL
    
    Args:
        val: Valor do pandas
        
    Returns:
        Valor convertido para tipo Python nativo
    """
    if pd.isna(val) or val is pd.NA or val is pd.NaT:
        return None
    elif isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    elif isinstance(val, (pd.Int64Dtype, pd.Int32Dtype)):
        return int(val) if pd.notna(val) else None
    elif isinstance(val, (pd.Float64Dtype, pd.Float32Dtype)):
        return float(val) if pd.notna(val) else None
    elif isinstance(val, pd.Period):
        return str(val)
    elif isinstance(val, (bool, np.bool_)):
        return bool(val)
    elif isinstance(val, (int, np.integer)):
        return int(val)
    elif isinstance(val, (float, np.floating)):
        return float(val)
    elif isinstance(val, str):
        return str(val)
    else:
        return val


class DatabaseLoader:
    """
    Classe para gerenciar carregamento de dados no PostgreSQL
    """
    
    def __init__(self, connection_params: Dict):
        """
        Inicializa o loader com parâmetros de conexão
        
        Args:
            connection_params: Dicionário com host, database, user, password, port
        """
        self.connection_params = connection_params
        self.conn = None
    
    def connect(self):
        """Estabelece conexão com o banco"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.conn.autocommit = False
            logger.info("Conexão com PostgreSQL estabelecida")
        except Exception as e:
            logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
            raise
    
    def close(self):
        """Fecha conexão com o banco"""
        if self.conn:
            self.conn.close()
            logger.info("Conexão com PostgreSQL fechada")
    
    def execute_sql(self, query: str, params: tuple = None):
        """Executa SQL e retorna resultados"""
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
    
    def create_schemas(self):
        """
        Cria schemas staging e analytics se não existirem
        """
        logger.info("Criando schemas...")
        
        schemas = ['staging', 'analytics']
        
        for schema in schemas:
            query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(schema)
            )
            with self.conn.cursor() as cur:
                cur.execute(query)
        
        self.conn.commit()
        logger.info("Schemas criados com sucesso")
    
    def create_staging_tables(self):
        """
        Cria tabelas de staging com metadados (source, load_timestamp)
        """
        logger.info("Criando tabelas de staging...")
        
        staging_tables = {
            'customers': """
                CREATE TABLE IF NOT EXISTS staging.customers (
                    customer_id VARCHAR(255) PRIMARY KEY,
                    customer_unique_id VARCHAR(255),
                    customer_zip_code_prefix INTEGER,
                    customer_city VARCHAR(255),
                    customer_state VARCHAR(2),
                    source VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'geolocation': """
                CREATE TABLE IF NOT EXISTS staging.geolocation (
                    geolocation_zip_code_prefix INTEGER,
                    geolocation_lat DOUBLE PRECISION,
                    geolocation_lng DOUBLE PRECISION,
                    geolocation_city VARCHAR(255),
                    geolocation_state VARCHAR(2),
                    source VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
                )
            """,
            'order_items': """
                CREATE TABLE IF NOT EXISTS staging.order_items (
                    order_id VARCHAR(255),
                    order_item_id INTEGER,
                    product_id VARCHAR(255),
                    seller_id VARCHAR(255),
                    shipping_limit_date TIMESTAMP,
                    price DOUBLE PRECISION,
                    freight_value DOUBLE PRECISION,
                    source VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (order_id, order_item_id)
                )
            """,
            'order_payments': """
                CREATE TABLE IF NOT EXISTS staging.order_payments (
                    order_id VARCHAR(255),
                    payment_sequential INTEGER,
                    payment_type VARCHAR(50),
                    payment_installments INTEGER,
                    payment_value DOUBLE PRECISION,
                    source VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (order_id, payment_sequential)
                )
            """,
            'order_reviews': """
                CREATE TABLE IF NOT EXISTS staging.order_reviews (
                    review_id VARCHAR(255) PRIMARY KEY,
                    order_id VARCHAR(255),
                    review_score INTEGER,
                    review_comment_title TEXT,
                    review_comment_message TEXT,
                    review_creation_date TIMESTAMP,
                    review_answer_timestamp TIMESTAMP,
                    source VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'orders': """
                CREATE TABLE IF NOT EXISTS staging.orders (
                    order_id VARCHAR(255) PRIMARY KEY,
                    customer_id VARCHAR(255),
                    order_status VARCHAR(50),
                    order_purchase_timestamp TIMESTAMP,
                    order_approved_at TIMESTAMP,
                    order_delivered_carrier_date TIMESTAMP,
                    order_delivered_customer_date TIMESTAMP,
                    order_estimated_delivery_date TIMESTAMP,
                    source VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'products': """
                CREATE TABLE IF NOT EXISTS staging.products (
                    product_id VARCHAR(255) PRIMARY KEY,
                    product_category_name VARCHAR(255),
                    product_category_name_english VARCHAR(255),
                    product_name_lenght DOUBLE PRECISION,
                    product_description_lenght DOUBLE PRECISION,
                    product_photos_qty DOUBLE PRECISION,
                    product_weight_g DOUBLE PRECISION,
                    product_length_cm DOUBLE PRECISION,
                    product_height_cm DOUBLE PRECISION,
                    product_width_cm DOUBLE PRECISION,
                    source VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'sellers': """
                CREATE TABLE IF NOT EXISTS staging.sellers (
                    seller_id VARCHAR(255) PRIMARY KEY,
                    seller_zip_code_prefix INTEGER,
                    seller_city VARCHAR(255),
                    seller_state VARCHAR(2),
                    source VARCHAR(255),
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        }
        
        with self.conn.cursor() as cur:
            for table_name, create_query in staging_tables.items():
                cur.execute(create_query)
        
        self.conn.commit()
        logger.info("Tabelas de staging criadas com sucesso")
    
    def create_star_schema_tables(self):
        """
        Cria tabelas do modelo estrela (star schema)
        """
        logger.info("Criando tabelas do modelo estrela...")
        
        # Dimensão: Tempo
        dim_time = """
            CREATE TABLE IF NOT EXISTS analytics.dim_time (
                time_id SERIAL PRIMARY KEY,
                order_date DATE NOT NULL,
                order_year INTEGER,
                order_month INTEGER,
                order_quarter INTEGER,
                order_day_of_week INTEGER,
                order_day_name VARCHAR(20),
                UNIQUE(order_date)
            )
        """
        
        # Dimensão: Clientes
        dim_customers = """
            CREATE TABLE IF NOT EXISTS analytics.dim_customers (
                customer_key SERIAL PRIMARY KEY,
                customer_id VARCHAR(255) UNIQUE NOT NULL,
                customer_unique_id VARCHAR(255),
                customer_state VARCHAR(2),
                customer_city VARCHAR(255),
                is_recurring_customer BOOLEAN DEFAULT FALSE,
                total_orders INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        # Dimensão: Produtos
        dim_products = """
            CREATE TABLE IF NOT EXISTS analytics.dim_products (
                product_key SERIAL PRIMARY KEY,
                product_id VARCHAR(255) UNIQUE NOT NULL,
                product_category_name VARCHAR(255),
                product_category_name_english VARCHAR(255),
                product_weight_g DOUBLE PRECISION,
                product_length_cm DOUBLE PRECISION,
                product_height_cm DOUBLE PRECISION,
                product_width_cm DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        # Dimensão: Vendedores
        dim_sellers = """
            CREATE TABLE IF NOT EXISTS analytics.dim_sellers (
                seller_key SERIAL PRIMARY KEY,
                seller_id VARCHAR(255) UNIQUE NOT NULL,
                seller_state VARCHAR(2),
                seller_city VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        # Dimensão: Geografia
        dim_geography = """
            CREATE TABLE IF NOT EXISTS analytics.dim_geography (
                geography_key SERIAL PRIMARY KEY,
                state VARCHAR(2) NOT NULL,
                city VARCHAR(255),
                zip_code_prefix INTEGER,
                UNIQUE(state, city, zip_code_prefix)
            )
        """
        
        # Tabela Fato: Pedidos
        fact_orders = """
            CREATE TABLE IF NOT EXISTS analytics.fact_orders (
                order_id VARCHAR(255) PRIMARY KEY,
                time_id INTEGER REFERENCES analytics.dim_time(time_id),
                customer_key INTEGER REFERENCES analytics.dim_customers(customer_key),
                product_key INTEGER,  -- Categoria principal do pedido
                seller_key INTEGER,  -- Vendedor principal
                geography_key INTEGER REFERENCES analytics.dim_geography(geography_key),
                
                -- Métricas do pedido
                order_status VARCHAR(50),
                order_items_count INTEGER,
                order_total_value DOUBLE PRECISION,
                order_items_total_price DOUBLE PRECISION,
                order_items_total_freight DOUBLE PRECISION,
                
                -- Métricas de entrega
                delivery_time_days INTEGER,
                delivery_delay_days INTEGER,
                
                -- Métricas de pagamento
                total_payment_value DOUBLE PRECISION,
                payment_types VARCHAR(255),
                max_installments INTEGER,
                
                -- Métricas de avaliação
                avg_review_score DOUBLE PRECISION,
                has_review_comment BOOLEAN,
                
                -- Metadados
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        # Índices para otimização
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_fact_orders_time ON analytics.fact_orders(time_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON analytics.fact_orders(customer_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_orders_geography ON analytics.fact_orders(geography_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON analytics.fact_orders(order_status)",
            "CREATE INDEX IF NOT EXISTS idx_dim_time_date ON analytics.dim_time(order_date)",
            "CREATE INDEX IF NOT EXISTS idx_dim_customers_id ON analytics.dim_customers(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_dim_products_id ON analytics.dim_products(product_id)",
            "CREATE INDEX IF NOT EXISTS idx_dim_sellers_id ON analytics.dim_sellers(seller_id)"
        ]
        
        with self.conn.cursor() as cur:
            cur.execute(dim_time)
            cur.execute(dim_customers)
            cur.execute(dim_products)
            cur.execute(dim_sellers)
            cur.execute(dim_geography)
            cur.execute(fact_orders)
            
            for index_query in indexes:
                cur.execute(index_query)
        
        self.conn.commit()
        logger.info("Tabelas do modelo estrela criadas com sucesso")
    
    def load_to_staging(self, datasets: Dict[str, pd.DataFrame], source: str = 'csv'):
        """
        Carrega dados para staging com metadados
        
        Args:
            datasets: Dicionário com datasets
            source: Identificador da fonte de dados
        """
        logger.info("=" * 80)
        logger.info("CARREGANDO DADOS PARA STAGING")
        logger.info("=" * 80)
        
        # Configuração por tabela: nome, colunas esperadas, chave primária
        table_config = {
            'customers': {
                'table': 'customers',
                'columns': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 
                          'customer_city', 'customer_state'],
                'pk': ['customer_id']
            },
            'geolocation': {
                'table': 'geolocation',
                'columns': ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng',
                           'geolocation_city', 'geolocation_state'],
                'pk': ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']
            },
            'order_items': {
                'table': 'order_items',
                'columns': ['order_id', 'order_item_id', 'product_id', 'seller_id',
                           'shipping_limit_date', 'price', 'freight_value'],
                'pk': ['order_id', 'order_item_id']
            },
            'order_payments': {
                'table': 'order_payments',
                'columns': ['order_id', 'payment_sequential', 'payment_type', 
                           'payment_installments', 'payment_value'],
                'pk': ['order_id', 'payment_sequential']
            },
            'order_reviews': {
                'table': 'order_reviews',
                'columns': ['review_id', 'order_id', 'review_score', 'review_comment_title',
                           'review_comment_message', 'review_creation_date', 'review_answer_timestamp'],
                'pk': ['review_id']
            },
            'orders': {
                'table': 'orders',
                'columns': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
                           'order_approved_at', 'order_delivered_carrier_date', 
                           'order_delivered_customer_date', 'order_estimated_delivery_date'],
                'pk': ['order_id']
            },
            'products': {
                'table': 'products',
                'columns': ['product_id', 'product_category_name', 'product_category_name_english',
                           'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 
                           'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'],
                'pk': ['product_id']
            },
            'sellers': {
                'table': 'sellers',
                'columns': ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state'],
                'pk': ['seller_id']
            }
        }
        
        load_timestamp = datetime.now()
        
        for dataset_name, df in datasets.items():
            if dataset_name not in table_config:
                continue
            
            config = table_config[dataset_name]
            table_name = config['table']
            logger.info(f"Carregando {dataset_name} para staging.{table_name}...")
            
            try:
                # Filtrar colunas esperadas
                available_columns = [col for col in config['columns'] if col in df.columns]
                if not available_columns:
                    logger.warning(f"  ⚠ Nenhuma coluna esperada encontrada")
                    continue
                
                df_staging = df[available_columns].copy()
                
                # Remover duplicatas baseado na chave primária
                pk_cols = [col for col in config['pk'] if col in df_staging.columns]
                if pk_cols:
                    initial_count = len(df_staging)
                    df_staging = df_staging.drop_duplicates(subset=pk_cols, keep='first')
                    if len(df_staging) < initial_count:
                        logger.info(f"  Removidas {initial_count - len(df_staging):,} duplicatas")
                
                # Adicionar metadados
                df_staging['source'] = source
                df_staging['load_timestamp'] = load_timestamp
                
                if len(df_staging) == 0:
                    logger.warning(f"  ⚠ Dataset vazio após processamento")
                    continue
                
                # Converter para lista de tuplas
                columns = list(df_staging.columns)
                values = [tuple(convert_pandas_value(row[col]) for col in columns) 
                         for _, row in df_staging.iterrows()]
                
                # Inserir dados
                with self.conn.cursor() as cur:
                    # Limpar dados existentes desta fonte
                    cur.execute(
                        sql.SQL("DELETE FROM staging.{} WHERE source = %s").format(
                            sql.Identifier(table_name)
                        ),
                        (source,)
                    )
                    
                    # Inserção em lote
                    insert_query = sql.SQL("INSERT INTO staging.{} ({}) VALUES %s").format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(map(sql.Identifier, columns))
                    )
                    
                    execute_values(cur, insert_query, values, page_size=1000)
                    self.conn.commit()
                    
                    logger.info(f"  ✓ {len(values):,} registros inseridos")
                    
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Erro ao carregar {dataset_name}: {e}")
                raise
        
        logger.info("Carregamento para staging concluído")
    
    def load_dim_time(self, df_orders: pd.DataFrame):
        """
        Carrega dimensão de tempo
        """
        logger.info("Carregando dimensão de tempo...")
        
        # Extrair datas únicas dos pedidos
        if 'order_purchase_timestamp' not in df_orders.columns:
            logger.warning("order_purchase_timestamp não encontrado")
            return
        
        df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])
        df_orders['order_date'] = df_orders['order_purchase_timestamp'].dt.date
        
        time_data = df_orders[['order_date']].drop_duplicates().copy()
        time_data['order_year'] = pd.to_datetime(time_data['order_date']).dt.year
        time_data['order_month'] = pd.to_datetime(time_data['order_date']).dt.month
        time_data['order_quarter'] = pd.to_datetime(time_data['order_date']).dt.quarter
        time_data['order_day_of_week'] = pd.to_datetime(time_data['order_date']).dt.dayofweek
        time_data['order_day_name'] = pd.to_datetime(time_data['order_date']).dt.day_name()
        
        # Converter para lista de tuplas
        columns = ['order_date', 'order_year', 'order_month', 'order_quarter', 'order_day_of_week', 'order_day_name']
        values = [
            (
                row['order_date'] if pd.notna(row['order_date']) else None,
                int(row['order_year']) if pd.notna(row['order_year']) else None,
                int(row['order_month']) if pd.notna(row['order_month']) else None,
                int(row['order_quarter']) if pd.notna(row['order_quarter']) else None,
                int(row['order_day_of_week']) if pd.notna(row['order_day_of_week']) else None,
                str(row['order_day_name']) if pd.notna(row['order_day_name']) else None
            )
            for _, row in time_data.iterrows()
        ]
        
        with self.conn.cursor() as cur:
            for val_tuple in values:
                cur.execute("""
                    INSERT INTO analytics.dim_time 
                    (order_date, order_year, order_month, order_quarter, order_day_of_week, order_day_name)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_date) DO NOTHING
                """, val_tuple)
        
        self.conn.commit()
        logger.info(f"  ✓ {len(time_data)} registros processados na dimensão de tempo")
    
    def load_dim_customers(self, df_customers: pd.DataFrame):
        """
        Carrega dimensão de clientes
        """
        logger.info("Carregando dimensão de clientes...")
        
        customers_data = df_customers[[
            'customer_id', 'customer_unique_id', 'customer_state', 'customer_city',
            'is_recurring_customer', 'total_orders'
        ]].copy()
        
        # Converter para lista de tuplas
        values = [
            (
                str(row['customer_id']) if pd.notna(row['customer_id']) else None,
                str(row['customer_unique_id']) if pd.notna(row['customer_unique_id']) else None,
                str(row['customer_state']) if pd.notna(row['customer_state']) else None,
                str(row['customer_city']) if pd.notna(row['customer_city']) else None,
                bool(row['is_recurring_customer']) if pd.notna(row['is_recurring_customer']) else False,
                int(row['total_orders']) if pd.notna(row['total_orders']) else 0
            )
            for _, row in customers_data.iterrows()
        ]
        
        with self.conn.cursor() as cur:
            for val_tuple in values:
                cur.execute("""
                    INSERT INTO analytics.dim_customers 
                    (customer_id, customer_unique_id, customer_state, customer_city, 
                     is_recurring_customer, total_orders)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) 
                    DO UPDATE SET
                        customer_unique_id = EXCLUDED.customer_unique_id,
                        customer_state = EXCLUDED.customer_state,
                        customer_city = EXCLUDED.customer_city,
                        is_recurring_customer = EXCLUDED.is_recurring_customer,
                        total_orders = EXCLUDED.total_orders,
                        updated_at = CURRENT_TIMESTAMP
                """, val_tuple)
        
        self.conn.commit()
        logger.info(f"  ✓ {len(customers_data)} clientes carregados")
    
    def load_dim_products(self, df_products: pd.DataFrame):
        """
        Carrega dimensão de produtos
        """
        logger.info("Carregando dimensão de produtos...")
        
        product_cols = ['product_id', 'product_category_name', 'product_category_name_english',
                       'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
        
        products_data = df_products[product_cols].copy()
        
        # Converter para lista de tuplas
        values = [
            (
                str(row['product_id']) if pd.notna(row['product_id']) else None,
                str(row['product_category_name']) if pd.notna(row['product_category_name']) else None,
                str(row['product_category_name_english']) if pd.notna(row['product_category_name_english']) else None,
                float(row['product_weight_g']) if pd.notna(row['product_weight_g']) else None,
                float(row['product_length_cm']) if pd.notna(row['product_length_cm']) else None,
                float(row['product_height_cm']) if pd.notna(row['product_height_cm']) else None,
                float(row['product_width_cm']) if pd.notna(row['product_width_cm']) else None
            )
            for _, row in products_data.iterrows()
        ]
        
        with self.conn.cursor() as cur:
            for val_tuple in values:
                cur.execute("""
                    INSERT INTO analytics.dim_products 
                    (product_id, product_category_name, product_category_name_english,
                     product_weight_g, product_length_cm, product_height_cm, product_width_cm)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) 
                    DO UPDATE SET
                        product_category_name = EXCLUDED.product_category_name,
                        product_category_name_english = EXCLUDED.product_category_name_english,
                        product_weight_g = EXCLUDED.product_weight_g,
                        product_length_cm = EXCLUDED.product_length_cm,
                        product_height_cm = EXCLUDED.product_height_cm,
                        product_width_cm = EXCLUDED.product_width_cm,
                        updated_at = CURRENT_TIMESTAMP
                """, val_tuple)
        
        self.conn.commit()
        logger.info(f"  ✓ {len(products_data)} produtos carregados")
    
    def load_dim_sellers(self, df_sellers: pd.DataFrame):
        """
        Carrega dimensão de vendedores
        """
        logger.info("Carregando dimensão de vendedores...")
        
        sellers_data = df_sellers[['seller_id', 'seller_state', 'seller_city']].copy()
        
        # Converter para lista de tuplas
        values = [
            (
                str(row['seller_id']) if pd.notna(row['seller_id']) else None,
                str(row['seller_state']) if pd.notna(row['seller_state']) else None,
                str(row['seller_city']) if pd.notna(row['seller_city']) else None
            )
            for _, row in sellers_data.iterrows()
        ]
        
        with self.conn.cursor() as cur:
            for val_tuple in values:
                cur.execute("""
                    INSERT INTO analytics.dim_sellers 
                    (seller_id, seller_state, seller_city)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (seller_id) 
                    DO UPDATE SET
                        seller_state = EXCLUDED.seller_state,
                        seller_city = EXCLUDED.seller_city,
                        updated_at = CURRENT_TIMESTAMP
                """, val_tuple)
        
        self.conn.commit()
        logger.info(f"  ✓ {len(sellers_data)} vendedores carregados")
    
    def load_dim_geography(self, df_customers: pd.DataFrame, df_sellers: pd.DataFrame):
        """
        Carrega dimensão de geografia
        """
        logger.info("Carregando dimensão de geografia...")
        
        # Combinar geografia de clientes e vendedores
        geo_customers = df_customers[['customer_state', 'customer_city', 'customer_zip_code_prefix']].copy()
        geo_customers.columns = ['state', 'city', 'zip_code_prefix']
        
        geo_sellers = df_sellers[['seller_state', 'seller_city', 'seller_zip_code_prefix']].copy()
        geo_sellers.columns = ['state', 'city', 'zip_code_prefix']
        
        geo_data = pd.concat([geo_customers, geo_sellers], ignore_index=True)
        geo_data = geo_data.drop_duplicates(subset=['state', 'city', 'zip_code_prefix'])
        geo_data = geo_data.fillna(0)  # Preencher zip_code_prefix nulo
        
        # Converter para lista de tuplas
        values = [
            (
                str(row['state']) if pd.notna(row['state']) else None,
                str(row['city']) if pd.notna(row['city']) else None,
                int(row['zip_code_prefix']) if pd.notna(row['zip_code_prefix']) else None
            )
            for _, row in geo_data.iterrows()
        ]
        
        with self.conn.cursor() as cur:
            for val_tuple in values:
                cur.execute("""
                    INSERT INTO analytics.dim_geography 
                    (state, city, zip_code_prefix)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (state, city, zip_code_prefix) DO NOTHING
                """, val_tuple)
        
        self.conn.commit()
        logger.info(f"  ✓ {len(geo_data)} registros geográficos carregados")
    
    def load_fact_orders(self, fact_table: pd.DataFrame):
        """
        Carrega tabela fato de pedidos
        """
        logger.info("Carregando tabela fato de pedidos...")
        
        # Preparar dados: obter foreign keys das dimensões
        fact_data = fact_table.copy()
        
        # Adicionar time_id
        fact_data['order_date'] = pd.to_datetime(fact_data['order_purchase_timestamp']).dt.date
        
        with self.conn.cursor() as cur:
            # Buscar time_id para cada data
            time_map = {}
            for date in fact_data['order_date'].unique():
                cur.execute("SELECT time_id FROM analytics.dim_time WHERE order_date = %s", (date,))
                result = cur.fetchone()
                if result:
                    time_map[date] = result[0]
            
            fact_data['time_id'] = fact_data['order_date'].map(time_map)
            
            # Buscar customer_key
            customer_map = {}
            if 'customer_id' in fact_data.columns:
                for customer_id in fact_data['customer_id'].dropna().unique():
                    cur.execute("SELECT customer_key FROM analytics.dim_customers WHERE customer_id = %s", (customer_id,))
                    result = cur.fetchone()
                    if result:
                        customer_map[customer_id] = result[0]
                
                fact_data['customer_key'] = fact_data['customer_id'].map(customer_map)
            
            # Buscar geography_key
            geography_map = {}
            if 'customer_state' in fact_data.columns and 'customer_city' in fact_data.columns:
                for idx, row in fact_data.iterrows():
                    state = str(row.get('customer_state', '') or '')
                    city = str(row.get('customer_city', '') or '')
                    if state and city:
                        cur.execute(
                            "SELECT geography_key FROM analytics.dim_geography WHERE state = %s AND city = %s LIMIT 1",
                            (state, city)
                        )
                        result = cur.fetchone()
                        if result:
                            geography_map[idx] = result[0]
            
            fact_data['geography_key'] = fact_data.index.map(geography_map)
            
            # Buscar product_key
            product_map = {}
            if 'main_product_id' in fact_data.columns:
                for product_id in fact_data['main_product_id'].dropna().unique():
                    cur.execute("SELECT product_key FROM analytics.dim_products WHERE product_id = %s", (product_id,))
                    result = cur.fetchone()
                    if result:
                        product_map[product_id] = result[0]
                
                fact_data['product_key'] = fact_data['main_product_id'].map(product_map)
            
            # Buscar seller_key
            seller_map = {}
            if 'main_seller_id' in fact_data.columns:
                for seller_id in fact_data['main_seller_id'].dropna().unique():
                    cur.execute("SELECT seller_key FROM analytics.dim_sellers WHERE seller_id = %s", (seller_id,))
                    result = cur.fetchone()
                    if result:
                        seller_map[seller_id] = result[0]
                
                fact_data['seller_key'] = fact_data['main_seller_id'].map(seller_map)
            
            # Selecionar colunas para inserção
            fact_columns = [
                'order_id', 'time_id', 'customer_key', 'product_key', 'seller_key', 'geography_key',
                'order_status', 'order_items_count', 'order_total_value',
                'order_items_total_price', 'order_items_total_freight',
                'delivery_time_days', 'delivery_delay_days',
                'total_payment_value', 'payment_types', 'max_installments',
                'avg_review_score', 'has_review_comment'
            ]
            
            # Preparar valores para inserção
            fact_columns_available = [col for col in fact_columns if col in fact_data.columns]
            fact_data_clean = fact_data[fact_columns_available].copy()
            
            # Converter para lista de tuplas
            values = []
            for _, row in fact_data_clean.iterrows():
                val_tuple = tuple(
                    convert_pandas_value(row[col]) if col in fact_data_clean.columns else None
                    for col in fact_columns
                )
                values.append(val_tuple)
            
            # Inserir/atualizar (idempotente)
            for val_tuple in values:
                cur.execute("""
                    INSERT INTO analytics.fact_orders 
                    (order_id, time_id, customer_key, product_key, seller_key, geography_key, order_status,
                     order_items_count, order_total_value, order_items_total_price, order_items_total_freight,
                     delivery_time_days, delivery_delay_days, total_payment_value, payment_types, max_installments,
                     avg_review_score, has_review_comment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) 
                    DO UPDATE SET
                        time_id = EXCLUDED.time_id,
                        customer_key = EXCLUDED.customer_key,
                        product_key = EXCLUDED.product_key,
                        seller_key = EXCLUDED.seller_key,
                        geography_key = EXCLUDED.geography_key,
                        order_status = EXCLUDED.order_status,
                        order_items_count = EXCLUDED.order_items_count,
                        order_total_value = EXCLUDED.order_total_value,
                        order_items_total_price = EXCLUDED.order_items_total_price,
                        order_items_total_freight = EXCLUDED.order_items_total_freight,
                        delivery_time_days = EXCLUDED.delivery_time_days,
                        delivery_delay_days = EXCLUDED.delivery_delay_days,
                        total_payment_value = EXCLUDED.total_payment_value,
                        payment_types = EXCLUDED.payment_types,
                        max_installments = EXCLUDED.max_installments,
                        avg_review_score = EXCLUDED.avg_review_score,
                        has_review_comment = EXCLUDED.has_review_comment,
                        updated_at = CURRENT_TIMESTAMP
                """, val_tuple)
        
        self.conn.commit()
        logger.info(f"  ✓ {len(fact_data)} pedidos carregados na tabela fato")
    
    def load_analytics(self, transformed: Dict[str, pd.DataFrame]):
        """
        Carrega dados transformados no modelo estrela
        
        Args:
            transformed: Dicionário com datasets transformados
        """
        logger.info("=" * 80)
        logger.info("CARREGANDO DADOS PARA MODELO ESTRELA")
        logger.info("=" * 80)
        
        # Carregar dimensões primeiro
        if 'orders' in transformed:
            self.load_dim_time(transformed['orders'])
        
        if 'customers' in transformed:
            self.load_dim_customers(transformed['customers'])
        
        if 'products' in transformed:
            self.load_dim_products(transformed['products'])
        
        if 'sellers' in transformed:
            self.load_dim_sellers(transformed['sellers'])
        
        if 'customers' in transformed and 'sellers' in transformed:
            self.load_dim_geography(transformed['customers'], transformed['sellers'])
        
        # Carregar tabela fato por último
        if 'fact_orders' in transformed:
            self.load_fact_orders(transformed['fact_orders'])
        
        logger.info("Carregamento para modelo estrela concluído")


def get_connection_params() -> Dict:
    """
    Obtém parâmetros de conexão do ambiente ou valores padrão
    
    Returns:
        Dicionário com parâmetros de conexão
    """
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'ecommerce_olist'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'postgres'),
        'port': os.getenv('DB_PORT', '5432')
    }


def load_all(datasets: Dict[str, pd.DataFrame], transformed: Dict[str, pd.DataFrame],
             connection_params: Dict = None):
    """
    Executa carregamento completo (staging + analytics)
    
    Args:
        datasets: Datasets brutos para staging
        transformed: Datasets transformados para analytics
        connection_params: Parâmetros de conexão (opcional)
    """
    if connection_params is None:
        connection_params = get_connection_params()
    
    loader = DatabaseLoader(connection_params)
    
    try:
        loader.connect()
        loader.create_schemas()
        loader.create_staging_tables()
        loader.create_star_schema_tables()
        
        # Carregar staging
        loader.load_to_staging(datasets, source='csv')
        
        # Carregar analytics
        loader.load_analytics(transformed)
        
        logger.info("=" * 80)
        logger.info("CARREGAMENTO CONCLUÍDO COM SUCESSO")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Erro no carregamento: {e}")
        raise
    finally:
        loader.close()


if __name__ == '__main__':
    # Teste básico
    logging.basicConfig(level=logging.INFO)
    
    print("Teste de carregamento - requer conexão PostgreSQL configurada")
    print("Use as variáveis de ambiente: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT")
