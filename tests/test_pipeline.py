"""
Testes para módulo de pipeline
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.pipeline import run_etl


@patch('src.pipeline.extract_all')
@patch('src.pipeline.transform_all')
def test_run_etl_success(mock_transform, mock_extract):
    """Testa execução bem-sucedida do pipeline"""
    # Mock dos dados
    mock_datasets = {
        'orders': MagicMock(),
        'customers': MagicMock()
    }
    
    mock_transformed = {
        'orders': MagicMock(),
        'fact_orders': MagicMock()
    }
    
    mock_extract.return_value = mock_datasets
    mock_transform.return_value = mock_transformed
    
    # Executar pipeline
    result = run_etl()
    
    # Verificar que funções foram chamadas
    assert mock_extract.called
    assert mock_transform.called
    
    # Verificar resultado
    assert 'transformed' in result
    assert result['transformed'] == mock_transformed


@patch('src.pipeline.extract_all')
def test_run_etl_extract_failure(mock_extract):
    """Testa pipeline com falha na extração"""
    mock_extract.return_value = {}
    
    result = run_etl()
    
    # Pipeline deve retornar resultado vazio mas não quebrar
    assert result is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
