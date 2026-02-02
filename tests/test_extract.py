"""
Testes para módulo de extração
"""

import pytest
import pandas as pd
from pathlib import Path
from src.extract import validate_schema, apply_dtypes, extract_csv


def test_validate_schema_success():
    """Testa validação de schema com colunas corretas"""
    df = pd.DataFrame({
        'customer_id': ['1', '2'],
        'customer_unique_id': ['a', 'b'],
        'customer_zip_code_prefix': [12345, 67890],
        'customer_city': ['São Paulo', 'Rio'],
        'customer_state': ['SP', 'RJ']
    })
    
    result = validate_schema(df, 'customers')
    assert result is True


def test_validate_schema_failure():
    """Testa validação de schema com colunas faltantes"""
    df = pd.DataFrame({
        'customer_id': ['1', '2'],
        'customer_unique_id': ['a', 'b']
        # Faltam outras colunas
    })
    
    result = validate_schema(df, 'customers')
    assert result is False


def test_apply_dtypes():
    """Testa aplicação de tipos de dados"""
    df = pd.DataFrame({
        'customer_id': ['1', '2'],
        'customer_zip_code_prefix': ['12345', '67890']
    })
    
    result = apply_dtypes(df, 'customers')
    
    assert result['customer_id'].dtype == 'string'
    assert pd.api.types.is_integer_dtype(result['customer_zip_code_prefix'])


def test_extract_csv_file_not_found():
    """Testa extração com arquivo inexistente"""
    result = extract_csv(Path('nonexistent_file.csv'), 'customers')
    assert result is None


def test_extract_csv_empty_file(tmp_path):
    """Testa extração com arquivo vazio"""
    empty_file = tmp_path / 'empty.csv'
    empty_file.write_text('')
    
    result = extract_csv(empty_file, 'customers')
    assert result is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
