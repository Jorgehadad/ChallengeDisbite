import pytest
from src.transform import DataTransformer

def test_transform_products(sample_config, sample_products, sample_transformed_products):
    transformer = DataTransformer(sample_config)
    result = transformer.transform_products(sample_products)
    
    assert result == sample_transformed_products
    assert 'rating' not in result[0]
    assert result[0]['rating_rate'] == 4.5

def test_generate_date_dimension():
    transformer = DataTransformer({})
    sales_data = [{'date': '2025-10-19', 'amount': 100}]
    
    dates = transformer.generate_date_dimension(sales_data)
    assert len(dates) == 1
    assert dates[0]['year'] == 2025
    assert dates[0]['month'] == 10