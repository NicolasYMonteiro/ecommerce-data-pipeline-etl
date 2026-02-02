"""
Script principal para execução do pipeline ETL
"""

import sys
from pathlib import Path

# Adicionar src ao path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / 'src'))

import logging
from pipeline import run_etl, verify_results

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == '__main__':
    print("=" * 80)
    print("E-COMMERCE DATA PIPELINE ETL")
    print("=" * 80)
    print()
    
    # Executar pipeline
    results = run_etl()
    
    # Verificar e exibir resultados
    if results:
        verify_results(results)
        print("\n✓ Pipeline executado com sucesso!")
    else:
        print("\n✗ Erro na execução do pipeline")
        sys.exit(1)
