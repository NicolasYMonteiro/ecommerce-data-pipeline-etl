"""
Script principal para execução do pipeline ETL
"""

import sys
from pathlib import Path
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env
project_root = Path(__file__).resolve().parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

# Adicionar src ao path
sys.path.insert(0, str(project_root / 'src'))

import logging
from pipeline import run_etl, verify_results

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

if __name__ == '__main__':
    import os
    
    print("=" * 80)
    print("E-COMMERCE DATA PIPELINE ETL")
    print("=" * 80)
    print()
    
    # Verificar se deve carregar no banco
    load_to_db = os.getenv('LOAD_TO_DB', 'false').lower() == 'true'
    
    if load_to_db:
        print("Modo: ETL completo (com carregamento no banco)")
        print("Configure variáveis: DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT")
        print()
        from pipeline import run_etl_complete
        results = run_etl_complete(load_to_db=True)
        
        if results and 'transformed' in results:
            verify_results(results['transformed'])
    else:
        print("Modo: ETL sem carregamento (apenas Extract + Transform)")
        print("Para carregar no banco, defina: export LOAD_TO_DB=true")
        print()
        from pipeline import run_etl, verify_results
        results = run_etl()
        
        if results and 'transformed' in results:
            verify_results(results['transformed'])
    
    # Verificar e exibir resultados
    if results:
        print("\n✓ Pipeline executado com sucesso!")
    else:
        print("\n✗ Erro na execução do pipeline")
        sys.exit(1)
