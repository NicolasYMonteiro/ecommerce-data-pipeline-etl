"""
Módulo de Configuração do Pipeline
Centraliza todas as configurações do pipeline ETL
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class Config:
    """Classe para gerenciar configurações do pipeline"""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._load_config()
    
    def _load_config(self):
        """Carrega configurações de arquivos YAML e variáveis de ambiente"""
        project_root = Path(__file__).resolve().parent.parent.parent
        
        # Carregar variáveis de ambiente
        env_path = project_root / '.env'
        if env_path.exists():
            load_dotenv(dotenv_path=env_path)
        
        # Carregar configurações YAML
        config_dir = project_root / 'config'
        self._config = {}
        
        # Pipeline config
        pipeline_config_path = config_dir / 'pipeline.yaml'
        if pipeline_config_path.exists():
            with open(pipeline_config_path, 'r', encoding='utf-8') as f:
                self._config.update(yaml.safe_load(f) or {})
        
        # Dataset config
        dataset_config_path = config_dir / 'dataset.yaml'
        if dataset_config_path.exists():
            with open(dataset_config_path, 'r', encoding='utf-8') as f:
                dataset_config = yaml.safe_load(f) or {}
                # O dataset.yaml tem 'datasets' como chave raiz, então extraímos o conteúdo
                if 'datasets' in dataset_config:
                    self._config['datasets'] = dataset_config['datasets']
                elif dataset_config:
                    # Se não tiver a chave 'datasets', assume que o conteúdo já é os datasets
                    self._config['datasets'] = dataset_config
        
        # Configurações padrão
        self._config.setdefault('pipeline', {})
        self._config.setdefault('paths', {})
        self._config.setdefault('database', {})
        self._config.setdefault('logging', {})
        
        # Paths padrão
        self._config['paths'] = {
            'data_dir': str(project_root / 'dataset' / 'raw'),
            'logs_dir': str(project_root / 'logs'),
            'project_root': str(project_root),
            **self._config.get('paths', {})
        }
        
        # Database config (prioridade: env > yaml > defaults)
        # Primeiro carrega valores do YAML como base
        db_yaml = self._config.get('database', {})
        # Depois sobrescreve com variáveis de ambiente (se existirem)
        self._config['database'] = {
            **db_yaml,  # Base do YAML
            'host': os.getenv('DB_HOST', db_yaml.get('host', 'localhost')),
            'port': int(os.getenv('DB_PORT', str(db_yaml.get('port', 5432)))),
            'name': os.getenv('DB_NAME', db_yaml.get('name', 'ecommerce_olist')),
            'user': os.getenv('DB_USER', db_yaml.get('user', 'postgres')),
            'password': os.getenv('DB_PASSWORD', db_yaml.get('password', 'postgres'))
        }
        
        # Pipeline behavior
        self._config['pipeline'] = {
            'load_to_db': os.getenv('LOAD_TO_DB', 'false').lower() == 'true',
            'batch_size': self._config.get('pipeline', {}).get('batch_size', 1000),
            'enable_validation': self._config.get('pipeline', {}).get('enable_validation', True),
            **self._config.get('pipeline', {})
        }
        
        # Logging config
        self._config['logging'] = {
            'level': self._config.get('logging', {}).get('level', 'INFO'),
            'format': self._config.get('logging', {}).get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            'file_logging': self._config.get('logging', {}).get('file_logging', True),
            'log_file': self._config.get('logging', {}).get('log_file', 'pipeline.log'),
            **self._config.get('logging', {})
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Obtém valor de configuração usando notação de ponto (ex: 'database.host')"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value if value is not None else default
    
    def get_path(self, key: str) -> Path:
        """Obtém um path de configuração como objeto Path"""
        path_str = self.get(f'paths.{key}')
        if path_str:
            return Path(path_str)
        return None
    
    @property
    def data_dir(self) -> Path:
        """Diretório dos dados"""
        return Path(self.get('paths.data_dir'))
    
    @property
    def logs_dir(self) -> Path:
        """Diretório de logs"""
        return Path(self.get('paths.logs_dir'))
    
    @property
    def database_config(self) -> Dict[str, Any]:
        """Configurações do banco de dados"""
        return self._config.get('database', {})
    
    @property
    def pipeline_config(self) -> Dict[str, Any]:
        """Configurações do pipeline"""
        return self._config.get('pipeline', {})
    
    @property
    def logging_config(self) -> Dict[str, Any]:
        """Configurações de logging"""
        return self._config.get('logging', {})


# Instância singleton
config = Config()
