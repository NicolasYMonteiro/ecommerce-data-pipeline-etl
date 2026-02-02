"""
Configurações compartilhadas para testes
"""

import pytest
import sys
from pathlib import Path

# Adicionar src ao path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / 'src'))


@pytest.fixture
def sample_orders_df():
    """DataFrame de exemplo para pedidos"""
    import pandas as pd
    return pd.DataFrame({
        'order_id': ['1', '2', '3'],
        'customer_id': ['c1', 'c2', 'c1'],
        'order_status': ['delivered', 'delivered', 'shipped'],
        'order_purchase_timestamp': ['2023-01-01', '2023-01-02', '2023-01-03']
    })


@pytest.fixture
def sample_order_items_df():
    """DataFrame de exemplo para itens de pedidos"""
    import pandas as pd
    return pd.DataFrame({
        'order_id': ['1', '1', '2'],
        'order_item_id': [1, 2, 1],
        'product_id': ['p1', 'p2', 'p1'],
        'seller_id': ['s1', 's1', 's2'],
        'price': [10.0, 20.0, 15.0],
        'freight_value': [5.0, 5.0, 3.0]
    })


@pytest.fixture
def temp_data_dir(tmp_path):
    """Diretório temporário para dados de teste"""
    data_dir = tmp_path / 'dataset' / 'raw'
    data_dir.mkdir(parents=True)
    return data_dir
