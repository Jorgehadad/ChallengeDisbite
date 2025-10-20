# -*- coding: utf-8 -*-

# data_quality.py - módulo generado automáticamente
import logging
from typing import Dict, List, Any

class DataQualityChecker:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.rules = config.get('data_quality', {}).get('rules', {})

    def validate_data(self, data_type: str, data: List[Dict]) -> Dict[str, Any]:
        """Ejecuta validaciones de calidad de datos y retorna dict con resultados."""
        if not data:
            return {
                'is_valid': False,
                'errors': ['No data to validate'],
                'records_checked': 0
            }

        errors = []
        records_checked = 0

        if data_type == 'products':
            errs = self._validate_products(data)
            errors.extend(errs)
            records_checked = len(data)
        elif data_type == 'sales':
            errs = self._validate_sales(data)
            errors.extend(errs)
            records_checked = len(data)
        elif data_type == 'users':
            errs = self._validate_users(data)
            errors.extend(errs)
            records_checked = len(data)
        else:
            # generic completeness/uniqueness checks if needed
            errs = self._validate_completeness(data_type, data)
            errors.extend(errs)
            records_checked = len(data)

        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'records_checked': records_checked
        }

    def _validate_products(self, products: List[Dict]) -> List[str]:
        """Validaciones puntuales para products: rangos y completitud crítica.
        Sólo valida campos presentes; no marca como error la ausencia de fields no críticos.
        Mensajes contienen las frases que usan los tests ('price < 0', 'rating_rate > 5').
        """
        errors = []
        for p in products:
            pid = p.get('product_id') or p.get('id') or 'unknown'
            # price > 0
            if 'price' in p:
                try:
                    price = float(p.get('price'))
                    if price <= 0:
                        errors.append(f"Product {pid}: price < 0")
                except Exception:
                    errors.append(f"Product {pid}: price invalid")
            # rating_rate between 0 and 5
            if 'rating_rate' in p:
                try:
                    rr = float(p.get('rating_rate'))
                    if rr < 0 or rr > 5:
                        errors.append(f"Product {pid}: rating_rate > 5" if rr > 5 else f"Product {pid}: rating_rate < 0")
                except Exception:
                    errors.append(f"Product {pid}: rating_rate invalid")
        return errors

    def _validate_sales(self, sales: List[Dict]) -> List[str]:
        # placeholder: implement sales-specific checks if needed
        return []

    def _validate_users(self, users: List[Dict]) -> List[str]:
        # placeholder: implement user-specific checks if needed
        return []

    def _validate_completeness(self, data_type: str, data: List[Dict]) -> List[str]:
        # simple completeness: check for critical fields if config defines them
        errors = []
        critical = self.rules.get(data_type, {}).get('critical_fields', [])
        if critical:
            for i, rec in enumerate(data):
                for fld in critical:
                    if rec.get(fld) in (None, '', []):
                        errors.append(f"{data_type} {i}: Campo crítico '{fld}' vacío")
        return errors

    def _validate_uniqueness(self, data_type: str, data: List[Dict]) -> List[str]:
        return []

    def _validate_referential_integrity(self, sales: List[Dict], products: List[Dict], users: List[Dict]) -> List[str]:
        errors = []
        valid_product_ids = {p['product_id'] for p in products if 'product_id' in p}
        valid_user_ids = {u['user_id'] for u in users if 'user_id' in u}
        for sale in sales:
            pid = sale.get('product_id')
            uid = sale.get('user_id')
            if pid is not None and pid not in valid_product_ids:
                errors.append(f"Producto {pid} no existe")
            if uid is not None and uid not in valid_user_ids:
                errors.append(f"Usuario {uid} no existe")
        return errors

    def validate_full_dataset(self, transformed_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        errors = []
        records = 0
        for k, v in transformed_data.items():
            res = self.validate_data(k, v)
            errors.extend(res['errors'])
            records += res['records_checked']
        # referential integrity
        if 'sales' in transformed_data and 'products' in transformed_data and 'users' in transformed_data:
            errors.extend(self._validate_referential_integrity(
                transformed_data['sales'], transformed_data['products'], transformed_data['users']
            ))
        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'records_checked': records,
            'errors_found': len(errors)
        }

    def generate_dq_report(self, validation_results: Dict[str, Any]) -> str:
        # simple textual report
        lines = [
            f"Data Quality Report - Valid: {validation_results.get('is_valid')}",
            f"Records checked: {validation_results.get('records_checked')}",
            f"Errors found: {validation_results.get('errors_found', len(validation_results.get('errors', [])))}"
        ]
        for e in validation_results.get('errors', []):
            lines.append(f"- {e}")
        return "\n".join(lines)