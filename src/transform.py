"""
Módulo de Transformação de Dados
Responsável por limpeza, padronização, enriquecimento e criação de métricas
"""

import pandas as pd
from typing import Dict
from datetime import datetime

try:
    from .utils.logger import get_logger
except ImportError:
    from utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# REGRAS DE NEGÓCIO
# ============================================================================

"""
REGRAS DE NEGÓCIO:

1. VALOR TOTAL DO PEDIDO:
   - O valor total do pedido é calculado a partir da soma dos itens + frete
   - Fonte: order_items (price + freight_value agrupados por order_id)
   - O dataset de pedidos não traz esse campo consolidado

2. TEMPO DE ENTREGA:
   - Calculado como diferença entre order_delivered_customer_date e order_purchase_timestamp
   - Valores negativos ou muito altos são tratados como outliers

3. CATEGORIAS DE PRODUTOS:
   - Produtos sem categoria recebem 'unknown'
   - Tradução de categorias aplicada quando disponível

4. STATUS DE PEDIDOS:
   - Apenas pedidos com status 'delivered' são considerados completos
   - Pedidos cancelados ou não entregues mantêm seus status originais

5. TRATAMENTO DE NULOS:
   - Valores numéricos: mantidos como NaN (não imputados)
   - Strings: substituídos por 'unknown' ou valores padrão apropriados
   - Datas: mantidas como NaT (Not a Time)

6. CLIENTES RECORRENTES:
   - Identificados através de customer_unique_id com múltiplos pedidos
   - Flag is_recurring_customer adicionada

7. COORDENADAS GEOGRÁFICAS:
   - Validadas para estar dentro dos limites do Brasil
   - Latitude: -33 a 5, Longitude: -73 a -32
"""


def standardize_columns(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Padroniza nomes de colunas (lowercase, snake_case)
    
    Args:
        df: DataFrame a ser padronizado
        dataset_name: Nome do dataset
        
    Returns:
        DataFrame com colunas padronizadas
    """
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()
    logger.debug(f"{dataset_name}: Colunas padronizadas")
    return df


def handle_missing_values(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Trata valores faltantes conforme regras de negócio
    
    Args:
        df: DataFrame a ser tratado
        dataset_name: Nome do dataset
        
    Returns:
        DataFrame com valores faltantes tratados
    """
    df = df.copy()
    
    # Tratamento específico por dataset
    if dataset_name == 'products':
        # Categorias vazias
        if 'product_category_name' in df.columns:
            df['product_category_name'] = df['product_category_name'].fillna('unknown')
        
        # Dimensões e peso: manter NaN (serão tratados na análise)
        # Não imputamos valores numéricos para não distorcer análises
    
    elif dataset_name == 'order_reviews':
        # Comentários podem ser nulos (opcional)
        if 'review_comment_title' in df.columns:
            df['review_comment_title'] = df['review_comment_title'].fillna('')
        if 'review_comment_message' in df.columns:
            df['review_comment_message'] = df['review_comment_message'].fillna('')
    
    elif dataset_name == 'orders':
        # Datas de entrega podem ser nulas (pedidos não entregues)
        # Mantemos como NaT para análise posterior
        pass
    
    # Strings genéricas: substituir por 'unknown'
    string_cols = df.select_dtypes(include=['string', 'object']).columns
    for col in string_cols:
        if df[col].isnull().any():
            null_count = df[col].isnull().sum()
            df[col] = df[col].fillna('unknown')
            logger.debug(f"{dataset_name}.{col}: {null_count} valores nulos substituídos por 'unknown'")
    
    return df


def convert_dates(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
    """
    Converte colunas de data para datetime
    
    Args:
        df: DataFrame
        date_columns: Lista de colunas de data
        
    Returns:
        DataFrame com datas convertidas
    """
    df = df.copy()
    
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
            logger.debug(f"Convertidas {df[col].notna().sum()} datas válidas em {col}")
    
    return df


def enrich_products(df_products: pd.DataFrame, df_category_translation: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece produtos com tradução de categorias
    
    Args:
        df_products: DataFrame de produtos
        df_category_translation: DataFrame de tradução
        
    Returns:
        DataFrame de produtos enriquecido
    """
    df = df_products.copy()
    
    # Merge com tradução
    df = df.merge(
        df_category_translation,
        on='product_category_name',
        how='left'
    )
    
    # Preencher categorias sem tradução
    df['product_category_name_english'] = df['product_category_name_english'].fillna(
        df['product_category_name'].fillna('unknown')
    )
    
    logger.info(f"Produtos enriquecidos: {df['product_category_name_english'].notna().sum()} categorias traduzidas")
    
    return df


def calculate_order_metrics(df_order_items: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas agregadas por pedido
    
    REGRA DE NEGÓCIO: O valor total do pedido é calculado a partir da soma dos itens + frete,
    pois o dataset de pedidos não traz esse campo consolidado.
    
    Args:
        df_order_items: DataFrame de itens de pedidos
        
    Returns:
        DataFrame com métricas por pedido
    """
    metrics = df_order_items.groupby('order_id').agg({
        'price': 'sum',
        'freight_value': 'sum',
        'product_id': 'count',
        'order_item_id': 'max'  # Número de itens
    }).reset_index()
    
    metrics.columns = ['order_id', 'order_items_total_price', 'order_items_total_freight', 
                      'order_items_count', 'order_max_item_id']
    
    # Valor total do pedido (REGRA DE NEGÓCIO #1)
    metrics['order_total_value'] = metrics['order_items_total_price'] + metrics['order_items_total_freight']
    
    logger.info(f"Métricas calculadas para {len(metrics)} pedidos")
    logger.info(f"Valor total médio por pedido: R$ {metrics['order_total_value'].mean():.2f}")
    
    return metrics


def calculate_delivery_metrics(df_orders: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas de tempo de entrega
    
    REGRA DE NEGÓCIO: Tempo de entrega calculado como diferença entre data de entrega
    e data de compra. Valores negativos ou muito altos são tratados como outliers.
    
    Args:
        df_orders: DataFrame de pedidos
        
    Returns:
        DataFrame com métricas de entrega
    """
    df = df_orders.copy()
    
    # Garantir que datas estão convertidas
    date_cols = ['order_purchase_timestamp', 'order_delivered_customer_date', 
                 'order_estimated_delivery_date']
    df = convert_dates(df, date_cols)
    
    # Tempo de entrega em dias (REGRA DE NEGÓCIO #2)
    if 'order_delivered_customer_date' in df.columns and 'order_purchase_timestamp' in df.columns:
        df['delivery_time_days'] = (
            df['order_delivered_customer_date'] - df['order_purchase_timestamp']
        ).dt.days
        
        # Tratar outliers: valores negativos ou muito altos (>365 dias)
        df.loc[df['delivery_time_days'] < 0, 'delivery_time_days'] = pd.NA
        df.loc[df['delivery_time_days'] > 365, 'delivery_time_days'] = pd.NA
        
        valid_deliveries = df['delivery_time_days'].notna().sum()
        logger.info(f"Tempo de entrega calculado para {valid_deliveries} pedidos entregues")
    
    # Diferença entre estimado e real
    if 'order_estimated_delivery_date' in df.columns and 'order_delivered_customer_date' in df.columns:
        df['delivery_delay_days'] = (
            df['order_delivered_customer_date'] - df['order_estimated_delivery_date']
        ).dt.days
        
        # Pedidos entregues antes do prazo terão valores negativos (OK)
        logger.debug(f"Diferença entre estimado e real calculada")
    
    return df


def identify_recurring_customers(df_orders: pd.DataFrame, df_customers: pd.DataFrame) -> pd.DataFrame:
    """
    Identifica clientes recorrentes
    
    REGRA DE NEGÓCIO: Clientes que fizeram mais de 1 pedido são marcados como recorrentes
    
    Args:
        df_orders: DataFrame de pedidos
        df_customers: DataFrame de clientes
        
    Returns:
        DataFrame de clientes com flag de recorrência
    """
    df = df_customers.copy()
    
    # Contar pedidos por customer_unique_id
    customer_order_count = df_orders.merge(
        df_customers[['customer_id', 'customer_unique_id']],
        on='customer_id',
        how='left'
    ).groupby('customer_unique_id')['order_id'].count().reset_index()
    
    customer_order_count.columns = ['customer_unique_id', 'total_orders']
    customer_order_count['is_recurring_customer'] = customer_order_count['total_orders'] > 1
    
    # Merge com clientes
    df = df.merge(customer_order_count, on='customer_unique_id', how='left')
    df['total_orders'] = df['total_orders'].fillna(0).astype('Int64')
    df['is_recurring_customer'] = df['is_recurring_customer'].fillna(False)
    
    recurring_count = df['is_recurring_customer'].sum()
    logger.info(f"Clientes recorrentes identificados: {recurring_count:,}")
    
    return df


def validate_geolocation(df_geolocation: pd.DataFrame) -> pd.DataFrame:
    """
    Valida coordenadas geográficas do Brasil
    
    REGRA DE NEGÓCIO: Coordenadas devem estar dentro dos limites do Brasil
    Latitude: -33 a 5, Longitude: -73 a -32
    
    Args:
        df_geolocation: DataFrame de geolocalização
        
    Returns:
        DataFrame com flag de coordenadas válidas
    """
    df = df_geolocation.copy()
    
    if 'geolocation_lat' in df.columns and 'geolocation_lng' in df.columns:
        df['is_valid_coordinate'] = (
            df['geolocation_lat'].between(-33, 5) & 
            df['geolocation_lng'].between(-73, -32)
        )
        
        valid_count = df['is_valid_coordinate'].sum()
        logger.info(f"Coordenadas válidas: {valid_count:,} / {len(df):,} ({valid_count/len(df)*100:.1f}%)")
    
    return df


def create_fact_table(df_orders: pd.DataFrame, df_order_items: pd.DataFrame,
                     df_order_payments: pd.DataFrame, df_order_reviews: pd.DataFrame,
                     df_customers: pd.DataFrame, df_products: pd.DataFrame,
                     df_sellers: pd.DataFrame) -> pd.DataFrame:
    """
    Cria tabela fato consolidada com todos os joins necessários
    
    REGRA DE NEGÓCIO: Tabela fato contém uma linha por pedido com todas as informações
    agregadas (itens, pagamentos, avaliações)
    
    Args:
        df_orders: DataFrame de pedidos
        df_order_items: DataFrame de itens
        df_order_payments: DataFrame de pagamentos
        df_order_reviews: DataFrame de avaliações
        df_customers: DataFrame de clientes
        df_products: DataFrame de produtos
        df_sellers: DataFrame de vendedores
        
    Returns:
        DataFrame da tabela fato
    """
    logger.info("Criando tabela fato consolidada...")
    
    # 1. Métricas de pedidos (valor total, itens, etc)
    order_metrics = calculate_order_metrics(df_order_items)
    
    # 2. Métricas de entrega
    df_orders_enriched = calculate_delivery_metrics(df_orders)
    
    # 3. Agregações de pagamentos
    payment_metrics = df_order_payments.groupby('order_id').agg({
        'payment_value': 'sum',
        'payment_type': lambda x: ', '.join(x.unique()),  # Múltiplos tipos
        'payment_installments': 'max'
    }).reset_index()
    payment_metrics.columns = ['order_id', 'total_payment_value', 'payment_types', 'max_installments']
    
    # 4. Agregações de avaliações
    review_metrics = df_order_reviews.groupby('order_id').agg({
        'review_score': 'mean',
        'review_comment_message': lambda x: x.notna().any()  # Tem comentário?
    }).reset_index()
    review_metrics.columns = ['order_id', 'avg_review_score', 'has_review_comment']
    
    # 5. Join principal: orders + métricas
    fact_table = df_orders_enriched.merge(order_metrics, on='order_id', how='left')
    fact_table = fact_table.merge(payment_metrics, on='order_id', how='left')
    fact_table = fact_table.merge(review_metrics, on='order_id', how='left')
    
    # 6. Join com clientes
    fact_table = fact_table.merge(
        df_customers[['customer_id', 'customer_unique_id', 'customer_state', 'customer_city']],
        on='customer_id',
        how='left'
    )
    
    # 7. Agregações de produtos (categoria e produto principal do pedido)
    product_agg = df_order_items.merge(
        df_products[['product_id', 'product_category_name_english']],
        on='product_id',
        how='left'
    )
    product_agg['product_category_name_english'] = product_agg['product_category_name_english'].fillna('unknown')
    
    # Produto mais frequente no pedido (ou primeiro se empate)
    product_main = product_agg.groupby('order_id')['product_id'].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.iloc[0] if len(x) > 0 else None
    ).reset_index()
    product_main.columns = ['order_id', 'main_product_id']
    
    # Categoria mais comum
    product_category = product_agg.groupby('order_id')['product_category_name_english'].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'unknown'
    ).reset_index()
    product_category.columns = ['order_id', 'main_product_category']
    
    product_agg = product_main.merge(product_category, on='order_id', how='left')
    fact_table = fact_table.merge(product_agg, on='order_id', how='left')
    
    # 8. Agregações de vendedores (vendedor principal e quantidade)
    seller_main = df_order_items.groupby('order_id')['seller_id'].agg(
        lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.iloc[0] if len(x) > 0 else None
    ).reset_index()
    seller_main.columns = ['order_id', 'main_seller_id']
    
    seller_count = df_order_items.groupby('order_id')['seller_id'].nunique().reset_index()
    seller_count.columns = ['order_id', 'unique_sellers_count']
    
    seller_agg = seller_main.merge(seller_count, on='order_id', how='left')
    fact_table = fact_table.merge(seller_agg, on='order_id', how='left')
    
    logger.info(f"Tabela fato criada: {len(fact_table)} pedidos")
    
    return fact_table


def transform_all(datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Executa todas as transformações nos datasets
    
    Args:
        datasets: Dicionário com datasets extraídos
        
    Returns:
        Dicionário com datasets transformados
    """
    logger.info("=" * 80)
    logger.info("INICIANDO TRANSFORMAÇÃO DE DADOS")
    logger.info("=" * 80)
    
    start_time = datetime.now()
    transformed = {}
    
    # 1. Padronização de colunas
    logger.info("Padronizando colunas...")
    for name, df in datasets.items():
        datasets[name] = standardize_columns(df, name)
    
    # 2. Tratamento de valores faltantes
    logger.info("Tratando valores faltantes...")
    for name, df in datasets.items():
        datasets[name] = handle_missing_values(df, name)
    
    # 3. Conversão de datas
    logger.info("Convertendo datas...")
    date_columns_map = {
        'orders': ['order_purchase_timestamp', 'order_approved_at', 
                  'order_delivered_carrier_date', 'order_delivered_customer_date',
                  'order_estimated_delivery_date'],
        'order_items': ['shipping_limit_date'],
        'order_reviews': ['review_creation_date', 'review_answer_timestamp']
    }
    
    for name, date_cols in date_columns_map.items():
        if name in datasets:
            datasets[name] = convert_dates(datasets[name], date_cols)
    
    # 4. Enriquecimento de produtos
    if 'products' in datasets and 'category_translation' in datasets:
        logger.info("Enriquecendo produtos com tradução de categorias...")
        datasets['products'] = enrich_products(datasets['products'], datasets['category_translation'])
    
    # 5. Métricas de pedidos
    if 'order_items' in datasets:
        logger.info("Calculando métricas de pedidos...")
        order_metrics = calculate_order_metrics(datasets['order_items'])
        transformed['order_metrics'] = order_metrics
    
    # 6. Métricas de entrega
    if 'orders' in datasets:
        logger.info("Calculando métricas de entrega...")
        datasets['orders'] = calculate_delivery_metrics(datasets['orders'])
    
    # 7. Clientes recorrentes
    if 'orders' in datasets and 'customers' in datasets:
        logger.info("Identificando clientes recorrentes...")
        datasets['customers'] = identify_recurring_customers(datasets['orders'], datasets['customers'])
    
    # 8. Validação de geolocalização
    if 'geolocation' in datasets:
        logger.info("Validando coordenadas geográficas...")
        datasets['geolocation'] = validate_geolocation(datasets['geolocation'])
    
    # 9. Criar tabela fato
    required_for_fact = ['orders', 'order_items', 'order_payments', 'order_reviews',
                         'customers', 'products', 'sellers']
    if all(key in datasets for key in required_for_fact):
        logger.info("Criando tabela fato...")
        fact_table = create_fact_table(
            datasets['orders'],
            datasets['order_items'],
            datasets['order_payments'],
            datasets['order_reviews'],
            datasets['customers'],
            datasets['products'],
            datasets['sellers']
        )
        transformed['fact_orders'] = fact_table
    
    # Adicionar datasets transformados ao resultado
    transformed.update(datasets)
    
    elapsed_time = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 80)
    logger.info(f"TRANSFORMAÇÃO CONCLUÍDA em {elapsed_time:.2f} segundos")
    logger.info(f"Total de datasets transformados: {len(transformed)}")
    logger.info("=" * 80)
    
    return transformed


if __name__ == '__main__':
    # Teste básico
    from extract import extract_all
    
    print("Extraindo dados...")
    datasets = extract_all()
    
    if datasets:
        print("\nTransformando dados...")
        transformed = transform_all(datasets)
        print(f"\nDatasets transformados: {list(transformed.keys())}")
        
        if 'fact_orders' in transformed:
            print(f"\nTabela fato: {len(transformed['fact_orders'])} linhas")
            print(f"Colunas: {list(transformed['fact_orders'].columns)}")
