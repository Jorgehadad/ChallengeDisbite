import pytest
import os
import json

@pytest.fixture
def sample_config():
    return {
        'api': {
            'base_url': 'https://fakestoreapi.com',
            'endpoints': {
                'products': '/products',
                'carts': '/carts',
                'users': '/users'
            },
            'retry': {
                'max_retries': 3,
                'backoff_factor': 1,
                'status_forcelist': [500, 502, 503, 504]
            }
        },
        'database': {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_fakestore_dw',
            'user': 'postgres',
            'password': 'admin',
            'target_schema': 'public'
        }
    }

@pytest.fixture
def sample_products():
    return [
        {
            'id': 1,
            'title': 'Test Product',
            'price': 99.99,
            'category': 'electronics',
            'description': 'Test description',
            'image': 'http://test.com/image.jpg',
            'rating': {'rate': 4.5, 'count': 120}
        }
    ]

@pytest.fixture
def sample_transformed_products():
    return [
        {
            'product_id': 1,
            'title': 'Test Product',
            'price': 99.99,
            'category': 'electronics',
            'description': 'Test description',
            'image_url': 'http://test.com/image.jpg',
            'rating_rate': 4.5,
            'rating_count': 120
        }
    ]