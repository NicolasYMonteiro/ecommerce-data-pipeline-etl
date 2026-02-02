"""
Testes para módulo de transformação
"""

import pytest
import pandas as pd
from src.transform import (
    standardize_columns,
    handle_missing_values,
    calculate_order_metrics
)


def test_standardize_columns():
    """Testa padronização de nomes de colunas"""
    df = pd.DataFrame({
        'Customer ID': ['1', '2'],
        'Order Date': ['2023-01-01', '2023-01-02'],
        'Total Value': [100.0, 200.0]
    })
    
    result = standardize_columns(df)
    
    assert 'customer_id' in result.columns
    assert 'order_date' in result.columns
    assert 'total_value' in result.columns


def test_handle_missing_values_products():
    """Testa tratamento de valores faltantes em produtos"""
    df = pd.DataFrame({
        'product_category_name': [None, 'electronics', None],
        'product_id': ['1', '2', '3']
    })
    
    result = handle_missing_values(df, 'products')
    
    # Categorias nulas devem ser 'unknown'
    assert result['product_category_name'].isna().sum() == 0
    assert (result['product_category_name'] == 'unknown').sum() >= 1


def test_calculate_order_metrics():
    """Testa cálculo de métricas de pedidos"""
    df = pd.DataFrame({
        'order_id': ['1', '1', '2'],
        'price': [10.0, 20.0, 15.0],
        'freight_value': [5.0, 5.0, 3.0]
    })
    
    result = calculate_order_metrics(df)
    
    assert 'order_id' in result.columns
    assert 'order_total_value' in result.columns
    assert len(result) == 2  # 2 pedidos únicos
    
    # Pedido 1: (10 + 5) + (20 + 5) = 40
    order1_total = result[result['order_id'] == '1']['order_total_value'].iloc[0]
    assert order1_total == 40.0


def test_create_fact_table_structure():
    """Testa estrutura básica da tabela fato"""
    orders = pd.DataFrame({
        'order_id': ['1', '2'],
        'customer_id': ['c1', 'c2'],
        'order_status': ['delivered', 'delivered'],
        'order_purchase_timestamp': ['2023-01-01', '2023-01-02']
    })
    
    order_items = pd.DataFrame({
        'order_id': ['1', '1', '2'],
        'product_id': ['p1', 'p2', 'p1'],
        'seller_id': ['s1', 's1', 's2'],
        'price': [10.0, 20.0, 15.0],
        'freight_value': [5.0, 5.0, 3.0]
    })
    
    order_payments = pd.DataFrame({
        'order_id': ['1', '2'],
        'payment_value': [30.0, 18.0],
        'payment_type': ['credit_card', 'boleto'],
        'payment_installments': [1, 1]
    })
    
    order_reviews = pd.DataFrame({
        'order_id': ['1', '2'],
        'review_score': [5, 4],
        'review_comment_message': ['Great', None]
    })
    
    customers = pd.DataFrame({
        'customer_id': ['c1', 'c2'],
        'customer_state': ['SP', 'RJ'],
        'customer_city': ['São Paulo', 'Rio']
    })
    
    products = pd.DataFrame({
        'product_id': ['p1', 'p2'],
        'product_category_name_english': ['electronics', 'books']
    })
    
    sellers = pd.DataFrame({
        'seller_id': ['s1', 's2'],
        'seller_state': ['SP', 'RJ'],
        'seller_city': ['São Paulo', 'Rio']
    })
    
    from src.transform import create_fact_table
    
    fact_table = create_fact_table(
        orders, order_items, order_payments, order_reviews,
        customers, products, sellers
    )
    
    # Verificar colunas essenciais
    assert 'order_id' in fact_table.columns
    assert 'order_total_value' in fact_table.columns
    assert 'main_product_id' in fact_table.columns
    assert 'main_seller_id' in fact_table.columns
    assert len(fact_table) == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
