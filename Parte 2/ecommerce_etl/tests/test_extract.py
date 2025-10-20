import pytest
import requests
import responses
from src.extract import APIDataExtractor

@pytest.fixture
def mock_config():
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
        }
    }

@pytest.fixture
def sample_product_response():
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

@responses.activate
def test_fetch_endpoint_success(mock_config, sample_product_response):
    # Setup mock response
    responses.add(
        responses.GET,
        'https://fakestoreapi.com/products',
        json=sample_product_response,
        status=200
    )
    
    extractor = APIDataExtractor(mock_config)
    data = extractor.fetch_endpoint('products', '/products')
    
    assert len(data) == 1
    assert data[0]['id'] == 1
    assert data[0]['title'] == 'Test Product'

@responses.activate
def test_fetch_endpoint_retry(mock_config):
    # Add two responses - first fails, second succeeds
    responses.add(
        responses.GET,
        'https://fakestoreapi.com/products',
        status=503
    )
    responses.add(
        responses.GET,
        'https://fakestoreapi.com/products',
        json=[{'id': 1}],
        status=200
    )
    
    extractor = APIDataExtractor(mock_config)
    data = extractor.fetch_endpoint('products', '/products')
    
    assert len(responses.calls) == 2
    assert data[0]['id'] == 1

@responses.activate
def test_fetch_endpoint_error(mock_config):
    # Setup mock to always return error
    responses.add(
        responses.GET,
        'https://fakestoreapi.com/products',
        status=503,
        body="Service Unavailable"
    )
    
    extractor = APIDataExtractor(mock_config)
    with pytest.raises(requests.exceptions.RequestException):
        extractor.fetch_endpoint('products', '/products')