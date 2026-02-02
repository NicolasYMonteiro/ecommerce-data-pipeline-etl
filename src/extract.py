"""
Módulo de Extração de Dados
Responsável pela leitura e validação inicial dos arquivos CSV
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

try:
    from .utils.logger import get_logger
    from .utils.config import config
except ImportError:
    from utils.logger import get_logger
    from utils.config import config

logger = get_logger(__name__)


# Schemas esperados para cada dataset
SCHEMAS = {
    'customers': {
        'columns': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 
                   'customer_city', 'customer_state'],
        'dtypes': {
            'customer_id': 'string',
            'customer_unique_id': 'string',
            'customer_zip_code_prefix': 'Int64',
            'customer_city': 'string',
            'customer_state': 'string'
        }
    },
    'geolocation': {
        'columns': ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng',
                   'geolocation_city', 'geolocation_state'],
        'dtypes': {
            'geolocation_zip_code_prefix': 'Int64',
            'geolocation_lat': 'float64',
            'geolocation_lng': 'float64',
            'geolocation_city': 'string',
            'geolocation_state': 'string'
        }
    },
    'order_items': {
        'columns': ['order_id', 'order_item_id', 'product_id', 'seller_id',
                   'shipping_limit_date', 'price', 'freight_value'],
        'dtypes': {
            'order_id': 'string',
            'order_item_id': 'Int64',
            'product_id': 'string',
            'seller_id': 'string',
            'shipping_limit_date': 'string',
            'price': 'float64',
            'freight_value': 'float64'
        }
    },
    'order_payments': {
        'columns': ['order_id', 'payment_sequential', 'payment_type', 'payment_installments',
                   'payment_value'],
        'dtypes': {
            'order_id': 'string',
            'payment_sequential': 'Int64',
            'payment_type': 'string',
            'payment_installments': 'Int64',
            'payment_value': 'float64'
        }
    },
    'order_reviews': {
        'columns': ['review_id', 'order_id', 'review_score', 'review_comment_title',
                   'review_comment_message', 'review_creation_date', 'review_answer_timestamp'],
        'dtypes': {
            'review_id': 'string',
            'order_id': 'string',
            'review_score': 'Int64',
            'review_comment_title': 'string',
            'review_comment_message': 'string',
            'review_creation_date': 'string',
            'review_answer_timestamp': 'string'
        }
    },
    'orders': {
        'columns': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
                   'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date',
                   'order_estimated_delivery_date'],
        'dtypes': {
            'order_id': 'string',
            'customer_id': 'string',
            'order_status': 'string',
            'order_purchase_timestamp': 'string',
            'order_approved_at': 'string',
            'order_delivered_carrier_date': 'string',
            'order_delivered_customer_date': 'string',
            'order_estimated_delivery_date': 'string'
        }
    },
    'products': {
        'columns': ['product_id', 'product_category_name', 'product_name_lenght',
                   'product_description_lenght', 'product_photos_qty', 'product_weight_g',
                   'product_length_cm', 'product_height_cm', 'product_width_cm'],
        'dtypes': {
            'product_id': 'string',
            'product_category_name': 'string',
            'product_name_lenght': 'Float64',
            'product_description_lenght': 'Float64',
            'product_photos_qty': 'Float64',
            'product_weight_g': 'Float64',
            'product_length_cm': 'Float64',
            'product_height_cm': 'Float64',
            'product_width_cm': 'Float64'
        }
    },
    'sellers': {
        'columns': ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state'],
        'dtypes': {
            'seller_id': 'string',
            'seller_zip_code_prefix': 'Int64',
            'seller_city': 'string',
            'seller_state': 'string'
        }
    },
    'category_translation': {
        'columns': ['product_category_name', 'product_category_name_english'],
        'dtypes': {
            'product_category_name': 'string',
            'product_category_name_english': 'string'
        }
    }
}

# Mapeamento de nomes de arquivos
# Mapeamento de datasets (pode ser sobrescrito por config)
def _get_file_mapping():
    """Obtém mapeamento de arquivos do config ou usa padrão"""
    dataset_config = config.get('datasets', {})
    if dataset_config:
        return {name: info.get('file', f'olist_{name}_dataset.csv') 
                for name, info in dataset_config.items()}
    
    # Fallback para mapeamento padrão
    return {
    'customers': 'olist_customers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'order_reviews': 'olist_order_reviews_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'category_translation': 'product_category_name_translation.csv'
}


def validate_schema(df: pd.DataFrame, dataset_name: str) -> bool:
    """
    Valida se o DataFrame possui as colunas esperadas
    
    Args:
        df: DataFrame a ser validado
        dataset_name: Nome do dataset para validação
        
    Returns:
        True se válido, False caso contrário
    """
    if dataset_name not in SCHEMAS:
        logger.warning(f"Schema não definido para {dataset_name}")
        return True
    
    expected_columns = set(SCHEMAS[dataset_name]['columns'])
    actual_columns = set(df.columns)
    
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns
    
    if missing_columns:
        logger.error(f"{dataset_name}: Colunas faltantes: {missing_columns}")
        return False
    
    if extra_columns:
        logger.warning(f"{dataset_name}: Colunas extras encontradas: {extra_columns}")
    
    return True


def apply_dtypes(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Aplica tipos de dados iniciais ao DataFrame
    
    Args:
        df: DataFrame a ser tipado
        dataset_name: Nome do dataset
        
    Returns:
        DataFrame com tipos aplicados
    """
    if dataset_name not in SCHEMAS:
        return df
    
    dtypes = SCHEMAS[dataset_name]['dtypes']
    
    # Aplicar tipos apenas nas colunas que existem
    for col, dtype in dtypes.items():
        if col in df.columns:
            try:
                if dtype == 'string':
                    df[col] = df[col].astype('string')
                elif dtype in ['Int64', 'Float64']:
                    # Usar nullable integer/float
                    df[col] = df[col].astype(dtype.lower() if dtype == 'Int64' else 'float64')
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                logger.warning(f"{dataset_name}.{col}: Erro ao aplicar tipo {dtype}: {e}")
    
    return df


def log_volume(df: pd.DataFrame, dataset_name: str) -> None:
    """
    Registra informações de volume do dataset
    
    Args:
        df: DataFrame
        dataset_name: Nome do dataset
    """
    rows = len(df)
    cols = len(df.columns)
    memory_mb = df.memory_usage(deep=True).sum() / 1024**2
    missing_values = df.isnull().sum().sum()
    
    logger.info(f"{dataset_name.upper()}: {rows:,} linhas | {cols} colunas | "
                f"{memory_mb:.2f} MB | {missing_values:,} valores faltantes")


def extract_csv(file_path: Path, dataset_name: str) -> Optional[pd.DataFrame]:
    """
    Extrai dados de um arquivo CSV com validação
    
    Args:
        file_path: Caminho do arquivo CSV
        dataset_name: Nome do dataset
        
    Returns:
        DataFrame com os dados extraídos ou None em caso de erro
    """
    try:
        logger.info(f"Extraindo {dataset_name} de {file_path}")
        
        # Leitura do CSV
        df = pd.read_csv(file_path, low_memory=False)
        
        # Validação de schema
        if not validate_schema(df, dataset_name):
            logger.error(f"Falha na validação de schema para {dataset_name}")
            return None
        
        # Aplicar tipos
        df = apply_dtypes(df, dataset_name)
        
        # Logging de volume
        log_volume(df, dataset_name)
        
        return df
        
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {file_path}")
        return None
    except pd.errors.EmptyDataError:
        logger.error(f"Arquivo vazio: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Erro ao extrair {dataset_name}: {e}")
        return None

BASE_DIR = Path(__file__).resolve().parent.parent
def extract_all(data_path: Path = None) -> Dict[str, pd.DataFrame]:
    """
    Extrai todos os datasets CSV do diretório especificado
    
    Args:
        data_path: Caminho do diretório contendo os arquivos CSV
        
    Returns:
        Dicionário com todos os datasets extraídos
    """
    data_dir = Path(data_path)
    
    if not data_dir.exists():
        logger.error(f"Diretório não encontrado: {data_dir}")
        return {}
    
    logger.info(f"Iniciando extração de dados de {data_dir}")
    start_time = datetime.now()
    
    datasets = {}
    
    file_mapping = _get_file_mapping()
    for dataset_name, filename in file_mapping.items():
        file_path = data_dir / filename
        df = extract_csv(file_path, dataset_name)
        
        if df is not None:
            datasets[dataset_name] = df
        else:
            logger.warning(f"Falha ao extrair {dataset_name}")
    
    elapsed_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"Extração concluída em {elapsed_time:.2f} segundos")
    logger.info(f"Total de datasets extraídos: {len(datasets)}/{len(file_mapping)}")
    
    return datasets


if __name__ == '__main__':
    # Execução direta para testes
    datasets = extract_all()
    print(f"\nDatasets extraídos: {list(datasets.keys())}")
