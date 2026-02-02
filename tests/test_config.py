"""
Testes para módulo de configuração
"""

import pytest
import os
import tempfile
from pathlib import Path
from src.utils.config import Config


def test_config_singleton():
    """Testa que Config é singleton"""
    config1 = Config()
    config2 = Config()
    assert config1 is config2


def test_config_defaults():
    """Testa valores padrão de configuração"""
    config = Config()
    
    assert config.get('pipeline.batch_size') == 1000
    assert config.get('pipeline.enable_validation') is True
    assert config.get('logging.level') == 'INFO'


def test_config_paths():
    """Testa que paths são retornados como Path objects"""
    config = Config()
    
    data_dir = config.data_dir
    assert isinstance(data_dir, Path)
    
    logs_dir = config.logs_dir
    assert isinstance(logs_dir, Path)


def test_config_database():
    """Testa configurações de banco de dados"""
    config = Config()
    
    db_config = config.database_config
    assert 'host' in db_config
    assert 'port' in db_config
    assert 'name' in db_config
    assert 'user' in db_config
    assert 'password' in db_config


def test_config_env_override():
    """Testa que variáveis de ambiente sobrescrevem configurações"""
    original_host = os.environ.get('DB_HOST')
    
    try:
        os.environ['DB_HOST'] = 'test_host'
        # Recriar instância para recarregar config
        Config._instance = None
        Config._config = None
        config = Config()
        
        assert config.get('database.host') == 'test_host'
    finally:
        if original_host:
            os.environ['DB_HOST'] = original_host
        else:
            os.environ.pop('DB_HOST', None)
        # Restaurar instância
        Config._instance = None
        Config._config = None


def test_config_get_nested():
    """Testa acesso a valores aninhados"""
    config = Config()
    
    # Valor existente
    value = config.get('pipeline.batch_size')
    assert value == 1000
    
    # Valor não existente
    value = config.get('pipeline.nonexistent', 'default')
    assert value == 'default'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
