"""
Módulo de Orquestração do Pipeline ETL
Coordena as etapas de Extract, Transform e Load
"""

import logging
from typing import Dict
from datetime import datetime
from pathlib import Path
import pandas as pd

try:
    from .extract import extract_all
    from .transform import transform_all
except ImportError:
    # Para execução direta
    from extract import extract_all
    from transform import transform_all

logger = logging.getLogger(__name__)


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
    logger.info("\n[ETAPA 2/2] TRANSFORMATION")
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
    
    return transformed


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
    
    # Verificação da tabela fato
    if 'fact_orders' in transformed:
        fact = transformed['fact_orders']
        print("\n--- Tabela Fato (fact_orders) ---")
        print(f"Total de pedidos: {len(fact):,}")
        print(f"Colunas: {len(fact.columns)}")
        
        # Estatísticas básicas
        if 'order_total_value' in fact.columns:
            print(f"\nValor total dos pedidos:")
            print(f"  Média: R$ {fact['order_total_value'].mean():.2f}")
            print(f"  Mediana: R$ {fact['order_total_value'].median():.2f}")
            print(f"  Total: R$ {fact['order_total_value'].sum():,.2f}")
        
        if 'delivery_time_days' in fact.columns:
            valid_deliveries = fact['delivery_time_days'].notna().sum()
            if valid_deliveries > 0:
                print(f"\nTempo de entrega:")
                print(f"  Média: {fact['delivery_time_days'].mean():.1f} dias")
                print(f"  Mediana: {fact['delivery_time_days'].median():.0f} dias")
                print(f"  Pedidos entregues: {valid_deliveries:,}")
        
        if 'avg_review_score' in fact.columns:
            valid_reviews = fact['avg_review_score'].notna().sum()
            if valid_reviews > 0:
                print(f"\nAvaliações:")
                print(f"  Nota média: {fact['avg_review_score'].mean():.2f}")
                print(f"  Pedidos avaliados: {valid_reviews:,}")
        
        # Primeiras linhas
        print("\n--- Primeiras 5 linhas da tabela fato ---")
        print(fact.head().to_string())
    
    # Verificação de clientes recorrentes
    if 'customers' in transformed:
        customers = transformed['customers']
        if 'is_recurring_customer' in customers.columns:
            recurring = customers['is_recurring_customer'].sum()
            total = len(customers)
            print(f"\n--- Clientes Recorrentes ---")
            print(f"Total: {recurring:,} / {total:,} ({recurring/total*100:.1f}%)")
    
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
