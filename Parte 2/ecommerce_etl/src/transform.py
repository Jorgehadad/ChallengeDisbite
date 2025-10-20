# -*- coding: utf-8 -*-

# transform.py - módulo generado automáticamente
import pandas as pd
import logging
from typing import Dict, List, Any, Tuple
from datetime import datetime, date

class DataTransformer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def transform_products(self, products):
        """Normaliza y aplana productos."""
        self.logger.info("[TRANSFORM] products: normalizando categorias y aplanando rating")
        transformed = []
        for p in products:
            prod_id = p.get('id') or p.get('product_id')
            # keep title as-is, category lowercased (no capitalizing)
            category = p.get('category')
            if category is not None:
                category = category.lower()
            transformed.append({
                'product_id': prod_id,
                'title': p.get('title'),
                'category': category,
                'price': float(p.get('price')) if p.get('price') is not None else None,
                'description': p.get('description'),
                'image_url': p.get('image') or p.get('image_url'),
                'rating_rate': (p.get('rating') or {}).get('rate') if isinstance(p.get('rating'), dict) else p.get('rating_rate'),
                'rating_count': (p.get('rating') or {}).get('count') if isinstance(p.get('rating'), dict) else p.get('rating_count')
            })
        self.logger.info(f"[TRANSFORM] products: {len(transformed)} registros transformados (categorias normalizadas, rating aplanado)")
        return transformed

    def transform_users(self, users_data: List[Dict]) -> Dict[str, List[Dict]]:
        """Transforma datos de usuarios separando en users y geography."""
        self.logger.info("[TRANSFORM] users: aplanando address/geolocation y normalizando nombres/emails")
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
        self.logger.info("[TRANSFORM] carts->sales: aplanando items y calculando metricas derivadas (total_amount)")
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
        
        self.logger.info(f"[TRANSFORM] sales: {len(sales_transformed)} registros de ventas transformados (incluye total_amount)")
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
        """Genera dimensión de tiempo a partir de las ventas.
        Acepta sales_data con 'date' como str ISO (YYYY-MM-DD), date o datetime.
        """
        dates = []
        seen = set()

        for sale in sales_data:
            raw = sale.get('date')
            if raw is None:
                continue

            # Normalizar a objeto date
            dt_obj = None
            if isinstance(raw, date) and not isinstance(raw, datetime):
                dt_obj = raw
            elif isinstance(raw, datetime):
                dt_obj = raw.date()
            elif isinstance(raw, str):
                # intentar parse ISO / YYYY-MM-DD
                try:
                    dt_obj = datetime.fromisoformat(raw).date()
                except Exception:
                    try:
                        dt_obj = datetime.strptime(raw, '%Y-%m-%d').date()
                    except Exception:
                        # ignorar si no se puede parsear
                        continue
            else:
                continue

            if dt_obj in seen:
                continue
            seen.add(dt_obj)

            dates.append({
                'date_key': int(dt_obj.strftime('%Y%m%d')),
                'date': dt_obj,
                'day': dt_obj.day,
                'month': dt_obj.month,
                'year': dt_obj.year,
                'quarter': (dt_obj.month - 1) // 3 + 1,
                'iso_week': int(dt_obj.strftime('%V')) if hasattr(dt_obj, 'strftime') else None,
                'day_of_week': dt_obj.weekday(),
                'day_name': dt_obj.strftime('%A'),
                'month_name': dt_obj.strftime('%B')
            })

        return sorted(dates, key=lambda x: x['date_key'])
