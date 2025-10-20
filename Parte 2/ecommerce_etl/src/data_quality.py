# -*- coding: utf-8 -*-

# data_quality.py - módulo generado automáticamente
import logging
import pandas as pd
from typing import Dict, List, Any, Tuple

class DataQualityChecker:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.rules = config.get('data_quality', {}).get('rules', {})
    
    def validate_data(self, data_type: str, data: List[Dict]) -> Dict[str, Any]:
        """Ejecuta validaciones de calidad de datos."""
        if not data:
            return {'is_valid': True, 'errors': []}
        
        errors = []
        
        # Validaciones específicas por tipo de datos
        if data_type == 'products':
            errors.extend(self._validate_products(data))
        elif data_type == 'sales':
            errors.extend(self._validate_sales(data))
        elif data_type == 'users':
            errors.extend(self._validate_users(data))
        
        # Validaciones genéricas
        errors.extend(self._validate_completeness(data_type, data))
        errors.extend(self._validate_uniqueness(data_type, data))
        
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'records_checked': len(data),
            'errors_found': len(errors)
        }
    
    def _validate_products(self, products: List[Dict]) -> List[str]:
        """Valida datos de productos."""
        errors = []
        
        for product in products:
            product_id = product.get('product_id', 'unknown')
            
            # Precio positivo
            if product.get('price', 0) <= 0:
                errors.append(f"Producto {product_id}: Precio debe ser positivo")
            
            # Rating entre 0-5
            rating = product.get('rating_rate', 0)
            if not (0 <= rating <= 5):
                errors.append(f"Producto {product_id}: Rating fuera de rango (0-5)")
            
            # Categoría no vacía
            if not product.get('category', '').strip():
                errors.append(f"Producto {product_id}: Categoría vacía")
        
        return errors
    
    def _validate_sales(self, sales: List[Dict]) -> List[str]:
        """Valida datos de ventas."""
        errors = []
        
        for sale in sales:
            cart_id = sale.get('cart_id', 'unknown')
            product_id = sale.get('product_id', 'unknown')
            
            # Cantidad positiva
            if sale.get('quantity', 0) <= 0:
                errors.append(f"Venta cart {cart_id}, producto {product_id}: Cantidad inválida")
            
            # Monto total coherente
            quantity = sale.get('quantity', 0)
            unit_price = sale.get('unit_price', 0)
            total_amount = sale.get('total_amount', 0)
            
            expected_total = quantity * unit_price
            if abs(total_amount - expected_total) > 0.01:  # Tolerancia para decimales
                errors.append(f"Venta cart {cart_id}: Total amount incorrecto")
        
        return errors
    
    def _validate_users(self, users: List[Dict]) -> List[str]:
        """Valida datos de usuarios."""
        errors = []
        
        for user in users:
            user_id = user.get('user_id', 'unknown')
            
            # Email válido
            email = user.get('email', '')
            if '@' not in email:
                errors.append(f"Usuario {user_id}: Email inválido")
            
            # Nombre no vacío
            if not user.get('name_first', '').strip():
                errors.append(f"Usuario {user_id}: Nombre vacío")
        
        return errors
    
    def _validate_completeness(self, data_type: str, data: List[Dict]) -> List[str]:
        """Valida completitud de datos (sin valores nulos en campos críticos)."""
        errors = []
        critical_fields = {
            'products': ['product_id', 'title', 'category', 'price'],
            'users': ['user_id', 'email', 'name_first'],
            'sales': ['cart_id', 'user_id', 'product_id', 'quantity', 'unit_price'],
            'geography': ['user_id', 'city'],
            'dates': ['date_key', 'date']
        }
        
        fields_to_check = critical_fields.get(data_type, [])
        
        for record in data:
            record_id = record.get(f"{data_type[:-1]}_id", 'unknown')  # product_id, user_id, etc.
            
            for field in fields_to_check:
                if field not in record or record[field] is None:
                    errors.append(f"{data_type.title()} {record_id}: Campo crítico '{field}' vacío")
        
        return errors
    
    def _validate_uniqueness(self, data_type: str, data: List[Dict]) -> List[str]:
        """Valida unicidad de identificadores."""
        errors = []
        
        id_field = f"{data_type[:-1]}_id"  # product_id, user_id, etc.
        
        if id_field in data[0] if data else False:
            ids = [record[id_field] for record in data if id_field in record]
            unique_ids = set(ids)
            
            if len(ids) != len(unique_ids):
                duplicates = [id for id in unique_ids if ids.count(id) > 1]
                errors.append(f"{data_type.title():} IDs duplicados: {duplicates}")
        
        return errors
    
    def generate_dq_report(self, validation_results: Dict[str, Any]) -> str:
        """Genera reporte de calidad de datos."""
        report = [
            "REPORTE DE CALIDAD DE DATOS",
            "=" * 50,
            f"Registros verificados: {validation_results['records_checked']}",
            f"Errores encontrados: {validation_results['errors_found']}",
            f"Estado: {'PASS' if validation_results['is_valid'] else 'FAIL'}",
            ""
        ]
        
        if validation_results['errors']:
            report.append("ERRORES DETECTADOS:")
            for error in validation_results['errors']:
                report.append(f"  - {error}")
        
        return "\n".join(report)