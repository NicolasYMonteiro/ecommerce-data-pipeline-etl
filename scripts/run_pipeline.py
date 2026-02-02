"""
Script principal para execução do pipeline ETL
"""

import sys
from pathlib import Path

# Adicionar src ao path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / 'src'))

# Importar após adicionar ao path
from utils.logger import setup_logger
from utils.config import config
from pipeline import run_etl, verify_results, run_etl_complete

# Configurar logging
setup_logger('pipeline')

if __name__ == '__main__':
    import os
    
    print("=" * 80)
    print("E-COMMERCE DATA PIPELINE ETL")
    print("=" * 80)
    print()
    
    # Verificar se deve carregar no banco (do config)
    load_to_db = config.get('pipeline.load_to_db', False)
    
    if load_to_db:
        print("Modo: ETL completo (com carregamento no banco)")
        print(f"Banco: {config.get('database.host')}:{config.get('database.port')}/{config.get('database.name')}")
        print()
        results = run_etl_complete(load_to_db=True)
        
        if results and 'transformed' in results:
            verify_results(results['transformed'])
    else:
        print("Modo: ETL sem carregamento (apenas Extract + Transform)")
        print("Para carregar no banco, defina LOAD_TO_DB=true no .env ou config/pipeline.yaml")
        print()
        results = run_etl()
        
        if results and 'transformed' in results:
            verify_results(results['transformed'])
    
    # Verificar e exibir resultados
    if results:
        print("\n✓ Pipeline executado com sucesso!")
    else:
        print("\n✗ Erro na execução do pipeline")
        sys.exit(1)
