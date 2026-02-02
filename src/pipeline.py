"""
Módulo de Orquestração do Pipeline ETL
Coordena as etapas de Extract, Transform e Load
"""

from typing import Dict
from datetime import datetime
from pathlib import Path
import pandas as pd

try:
    from .extract import extract_all
    from .transform import transform_all
    from .load import load_all, get_connection_params
    from .utils.logger import get_logger
    from .utils.config import config
except ImportError:
    # Para execução direta
    from extract import extract_all
    from transform import transform_all
    from load import load_all, get_connection_params
    from utils.logger import get_logger
    from utils.config import config

logger = get_logger(__name__)


def run_etl(data_path: str = None) -> Dict:
    """
    Executa o pipeline ETL completo
    
    Args:
        data_path: Caminho do diretório com os dados (opcional)
        
    Returns:
        Dicionário com datasets transformados
    """
    logger.info("=" * 80)
    logger.info("INICIANDO PIPELINE ETL")
    logger.info("=" * 80)
    
    pipeline_start = datetime.now()
    
    # ETAPA 1: EXTRACT
    logger.info("\n[ETAPA 1/2] EXTRACTION")
    logger.info("-" * 80)
    
    if data_path:
        datasets = extract_all(Path(data_path))
    else:
        # Tentar dataset/raw primeiro, depois dataset/
        base_dir = Path(__file__).resolve().parent.parent
        data_dir = base_dir / 'dataset' / 'raw'
        if not data_dir.exists():
            data_dir = base_dir / 'dataset'
        datasets = extract_all(data_dir)
    
    if not datasets:
        logger.error("Falha na extração. Pipeline interrompido.")
        return {}
    
    logger.info(f"✓ Extração concluída: {len(datasets)} datasets")
    
    # ETAPA 2: TRANSFORM
    logger.info("\n[ETAPA 2/3] TRANSFORMATION")
    logger.info("-" * 80)
    
    transformed = transform_all(datasets)
    
    if not transformed:
        logger.error("Falha na transformação. Pipeline interrompido.")
        return {}
    
    logger.info(f"✓ Transformação concluída: {len(transformed)} datasets")
    
    # RESUMO FINAL
    pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
    
    logger.info("\n" + "=" * 80)
    logger.info("PIPELINE ETL CONCLUÍDO COM SUCESSO")
    logger.info("=" * 80)
    logger.info(f"Tempo total: {pipeline_elapsed:.2f} segundos")
    logger.info(f"Datasets processados: {len(transformed)}")
    
    if 'fact_orders' in transformed:
        logger.info(f"Tabela fato criada: {len(transformed['fact_orders'])} pedidos")
    
    return {'datasets': datasets, 'transformed': transformed}


def run_etl_complete(data_path: str = None, load_to_db: bool = True, 
                     connection_params: Dict = None) -> Dict:
    """
    Executa o pipeline ETL completo incluindo Load
    
    Args:
        data_path: Caminho do diretório com os dados (opcional)
        load_to_db: Se True, carrega dados no banco (padrão: True)
        connection_params: Parâmetros de conexão (opcional)
        
    Returns:
        Dicionário com datasets e resultados
    """
    # Executar Extract e Transform
    results = run_etl(data_path)
    
    if not results:
        return {}
    
    datasets = results.get('datasets', {})
    transformed = results.get('transformed', {})
    
    # ETAPA 3: LOAD (opcional)
    if load_to_db:
        logger.info("\n[ETAPA 3/3] LOAD")
        logger.info("-" * 80)
        
        try:
            if connection_params is None:
                connection_params = get_connection_params()
            
            load_all(datasets, transformed, connection_params)
            logger.info("✓ Carregamento concluído")
            
        except Exception as e:
            logger.error(f"Erro no carregamento: {e}")
            logger.warning("Pipeline continuou sem carregar no banco")
    
    return {'datasets': datasets, 'transformed': transformed}


def verify_results(transformed: Dict) -> None:
    """
    Verifica e exibe resultados do pipeline
    
    Args:
        transformed: Dicionário com datasets transformados
    """
    print("\n" + "=" * 80)
    print("VERIFICAÇÃO DE RESULTADOS")
    print("=" * 80)
    
    # Resumo por dataset
    print("\n--- Resumo dos Datasets ---")
    for name, df in transformed.items():
        if isinstance(df, pd.DataFrame):
            print(f"{name:30s} | {len(df):>10,} linhas | {len(df.columns):>3} colunas")
    
    print("\n" + "=" * 80)


if __name__ == '__main__':
    # Configuração de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Executar pipeline
    results = run_etl()
    
    # Verificar resultados
    if results:
        verify_results(results)
    else:
        print("\nErro: Pipeline não retornou resultados")
