# -*- coding: utf-8 -*-

# transform.py - módulo generado automáticamente
import pandas as pd
import logging
from typing import Dict, List, Any, Tuple
from datetime import datetime, timedelta
import numpy as np

class DataTransformer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def transform_products(self, products_data: List[Dict]) -> List[Dict]:
        """Transforma datos de productos."""
        transformed = []
        
        for product in products_data:
            try:
                # Aplanar estructura de rating
                rating = product.get('rating', {})
                
                transformed_product = {
                    'product_id': product['id'],
                    'title': product['title'].strip(),
                    'category': self._normalize_category(product['category']),
                    'price': float(product['price']),
                    'description': product.get('description', '').strip(),
                    'image_url': product.get('image', ''),
                    'rating_rate': float(rating.get('rate', 0)),
                    'rating_count': int(rating.get('count', 0)),
                    'created_at': datetime.now()
                }
                
                # Validaciones básicas
                if transformed_product['price'] < 0:
                    self.logger.warning(f"Precio negativo para producto {product['id']}")
                    continue
                    
                transformed.append(transformed_product)
                
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Error transformando producto {product.get('id', 'unknown')}: {str(e)}")
                continue
        
        self.logger.info(f"Transformados {len(transformed)} productos")
        return transformed
    
    def _normalize_category(self, category: str) -> str:
        """Normaliza categorías a formato consistente."""
        if not category:
            return 'Unknown'
        
        # Title case y limpieza
        normalized = category.strip().title()
        
        # Mapeo de categorías comunes
        category_mapping = {
            "Men'S Clothing": "Men's Clothing",
            "Women'S Clothing": "Women's Clothing",
            "Electronics": "Electronics",
            "Jewelery": "Jewelry"
        }
        
        return category_mapping.get(normalized, normalized)
    
    def transform_users(self, users_data: List[Dict]) -> Dict[str, List[Dict]]:
        """Transforma datos de usuarios separando en users y geography."""
        users_transformed = []
        geography_transformed = []
        
        for user in users_data:
            try:
                # Extraer nombre
                name = user.get('name', {})
                address = user.get('address', {})
                geolocation = address.get('geolocation', {})
                
                # Transformar usuario
                user_transformed = {
                    'user_id': user['id'],
                    'name_first': name.get('firstname', '').strip().title(),
                    'name_last': name.get('lastname', '').strip().title(),
                    'email': user.get('email', '').strip().lower(),
                    'username': user.get('username', '').strip(),
                    'phone': user.get('phone', ''),
                    'created_at': datetime.now()
                }
                users_transformed.append(user_transformed)
                
                # Transformar geografía
                geo_transformed = {
                    'user_id': user['id'],
                    'city': address.get('city', '').strip().title(),
                    'street': address.get('street', '').strip(),
                    'zipcode': address.get('zipcode', '').strip(),
                    'lat': float(geolocation.get('lat', 0)),
                    'lng': float(geolocation.get('long', 0)),
                    'created_at': datetime.now()
                }
                geography_transformed.append(geo_transformed)
                
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Error transformando usuario {user.get('id', 'unknown')}: {str(e)}")
                continue
        
        self.logger.info(f"Transformados {len(users_transformed)} usuarios y {len(geography_transformed)} registros geográficos")
        
        return {
            'users': users_transformed,
            'geography': geography_transformed
        }
    
    def transform_carts(self, carts_data: List[Dict], products_data: List[Dict]) -> List[Dict]:
        """Transforma datos de carritos en hechos de ventas."""
        sales_transformed = []
        
        # Crear mapeo de productos para búsqueda rápida
        products_map = {p['id']: p for p in products_data}
        
        for cart in carts_data:
            try:
                cart_date = self._parse_date(cart.get('date', ''))
                user_id = cart.get('userId')
                cart_id = cart.get('id')
                
                products = cart.get('products', [])
                
                for product_item in products:
                    product_id = product_item.get('productId')
                    quantity = product_item.get('quantity', 0)
                    
                    # Buscar información del producto
                    product_info = products_map.get(product_id)
                    if not product_info:
                        self.logger.warning(f"Producto {product_id} no encontrado para carrito {cart_id}")
                        continue
                    
                    unit_price = float(product_info.get('price', 0))
                    total_amount = quantity * unit_price
                    
                    sale_record = {
                        'cart_id': cart_id,
                        'user_id': user_id,
                        'product_id': product_id,
                        'date': cart_date,
                        'quantity': quantity,
                        'unit_price': unit_price,
                        'total_amount': total_amount,
                        'loaded_at': datetime.now()
                    }
                    
                    # Validar datos
                    if quantity <= 0 or unit_price < 0:
                        self.logger.warning(f"Datos inválidos en carrito {cart_id}, producto {product_id}")
                        continue
                    
                    sales_transformed.append(sale_record)
                    
            except (KeyError, ValueError, TypeError) as e:
                self.logger.error(f"Error transformando carrito {cart.get('id', 'unknown')}: {str(e)}")
                continue
        
        self.logger.info(f"Transformados {len(sales_transformed)} registros de ventas")
        return sales_transformed
    
    def _parse_date(self, date_string: str) -> datetime:
        """Convierte string de fecha a objeto datetime."""
        try:
            # Formato: "2020-02-03T00:00:00.000Z"
            if 'T' in date_string:
                return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            else:
                return datetime.strptime(date_string, '%Y-%m-%d')
        except (ValueError, TypeError):
            self.logger.warning(f"Fecha inválida: {date_string}, usando fecha actual")
            return datetime.now()
    
    def generate_date_dimension(self, sales_data):
        """Genera dimensión de tiempo a partir de las ventas."""
        dates = []
        unique_dates = set()
        
        for sale in sales_data:
            date = sale['date']  # Asumiendo que es un objeto datetime
            if date not in unique_dates:
                unique_dates.add(date)
                dates.append({
                    'date_key': int(date.strftime('%Y%m%d')),
                    'date': date,
                    'day': date.day,
                    'month': date.month,
                    'year': date.year,
                    'quarter': (date.month - 1) // 3 + 1,
                    'iso_week': int(date.strftime('%V')),
                    'day_of_week': date.weekday(),
                    'day_name': date.strftime('%A'),
                    'month_name': date.strftime('%B')
                })
        
        return sorted(dates, key=lambda x: x['date_key'])