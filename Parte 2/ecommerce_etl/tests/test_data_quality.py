import pytest
import os
import yaml
from src.data_quality import DataQualityChecker

@pytest.fixture
def test_config():
    config_path = os.path.join(os.path.dirname(__file__), 'test_config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def test_validate_products(test_config):
    checker = DataQualityChecker(test_config)
    products = [
        {'product_id': 1, 'price': 100, 'rating_rate': 4.5},
        {'product_id': 2, 'price': -10, 'rating_rate': 6}  # Invalid values
    ]
    
    validation = checker.validate_data('products', products)
    assert len(validation['errors']) == 2
    assert any('price < 0' in error for error in validation['errors'])
    assert any('rating_rate > 5' in error for error in validation['errors'])

def test_validate_referential_integrity(test_config):
    checker = DataQualityChecker(test_config)
    sales = [{'product_id': 999, 'user_id': 1}]  # Non-existent product
    products = [{'product_id': 1}]
    users = [{'user_id': 1}]
    
    errors = checker._validate_referential_integrity(sales, products, users)
    assert len(errors) == 1
    assert 'Producto 999 no existe' in errors[0]


def test_validate_products_duplicates_and_ranges(test_config):
    checker = DataQualityChecker(test_config)
    products = [
        {'product_id': 1, 'price': 10, 'rating_rate': 4.5},
        {'product_id': 2, 'price': -5, 'rating_rate': 6},
        {'product_id': 1, 'price': 12, 'rating_rate': 4}
    ]

    validation = checker.validate_data('products', products)
    assert validation['is_valid'] is False
    assert any('price < 0' in e for e in validation['errors'])
    assert any('rating_rate' in e for e in validation['errors'])
    assert any('Duplicado' in e for e in validation['errors'])


def test_validate_full_dataset_combined(test_config):
    checker = DataQualityChecker(test_config)
    users = [
        {'user_id': 1, 'email': 'a@example.com'},
        {'user_id': 2, 'email': ''}
    ]
    products = [
        {'product_id': 1},
    ]
    sales = [
        {'cart_id': 10, 'product_id': 1, 'user_id': 1, 'quantity': 2, 'unit_price': 5},
        {'cart_id': 11, 'product_id': 999, 'user_id': 2, 'quantity': 1, 'unit_price': 3},
        {'cart_id': 10, 'product_id': 1, 'user_id': 1, 'quantity': 2, 'unit_price': 5},
    ]

    full = {'users': users, 'products': products, 'sales': sales}
    res = checker.validate_full_dataset(full)
    assert res['is_valid'] is False
    assert any('email' in e or 'faltante' in e for e in res['errors'])
    assert any('no existe' in e for e in res['errors'])
    assert any('Duplicado' in e for e in res['errors'])

def test_validate_empty_data(test_config):
    checker = DataQualityChecker(test_config)
    validation = checker.validate_data('products', [])
    assert validation['is_valid'] is False
    assert 'No data to validate' in validation['errors'][0]