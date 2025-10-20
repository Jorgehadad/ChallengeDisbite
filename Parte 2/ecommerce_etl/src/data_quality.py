# -*- coding: utf-8 -*-

# data_quality.py - módulo generado automáticamente
import logging
from typing import Any, Dict, List, Tuple


class DataQualityChecker:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.rules = config.get('data_quality', {}).get('rules', {})
        # default thresholds
        self.null_threshold = self.config.get('data_quality', {}).get('null_threshold', 0.05)
        self.duplicate_threshold = self.config.get('data_quality', {}).get('duplicate_threshold', 0.0)

    def validate_data(self, data_type: str, data: List[Dict]) -> Dict[str, Any]:
        """Ejecuta validaciones por entidad y retorna resultados + metadata."""
        if not data:
            return {
                'is_valid': False,
                'errors': ['No data to validate'],
                'records_checked': 0,
                'details': []
            }

        errors: List[str] = []
        details: List[Dict[str, Any]] = []
        records_checked = len(data)

        if data_type == 'products':
            self.logger.info(
                "[DQ] products: validando completitud, rangos (price>0, rating_rate en 0-5) y duplicados por product_id"
            )
            errs, dets = self._validate_products(data)
        elif data_type == 'sales':
            self.logger.info(
                "[DQ] sales: validando completitud (quantity, price/total), rangos (quantity>0) y duplicados cart_id/product_id"
            )
            errs, dets = self._validate_sales(data)
        elif data_type == 'users':
            self.logger.info(
                "[DQ] users: validando completitud (user_id, email) y unicidad de user_id"
            )
            errs, dets = self._validate_users(data)
        else:
            self.logger.info(f"[DQ] {data_type}: validando completitud segun campos criticos de config")
            errs, dets = self._validate_completeness(data_type, data)

        errors.extend(errs)
        details.extend(dets)

        return {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'records_checked': records_checked,
            'details': details
        }

    def _validate_products(self, products: List[Dict]) -> Tuple[List[str], List[Dict[str, Any]]]:
        """Validaciones puntuales para products."""
        errors: List[str] = []
        details: List[Dict[str, Any]] = []

        field_counts: Dict[str, int] = {}
        total = len(products)
        for p in products:
            for k, v in p.items():
                field_counts.setdefault(k, 0)
                if v not in (None, '', []):
                    field_counts[k] += 1

        for fld, cnt in field_counts.items():
            null_ratio = 1.0 - (cnt / total) if total else 0.0
            if null_ratio >= self.null_threshold:
                msg = f"products: Campo '{fld}' tiene {null_ratio:.2%} nulos"
                errors.append(msg)
                details.append({
                    'dataset': 'products',
                    'field': fld,
                    'issue': 'null_ratio',
                    'threshold': self.null_threshold,
                    'message': msg
                })

        for idx, p in enumerate(products):
            pid = p.get('product_id') or p.get('id') or 'unknown'
            price_raw = p.get('price')
            if 'price' in p:
                try:
                    price = float(price_raw)
                    if price <= 0:
                        msg = f"Product {pid}: price < 0"
                        errors.append(msg)
                        details.append({
                            'dataset': 'products',
                            'record_id': pid,
                            'record_index': idx,
                            'field': 'price',
                            'issue': 'price<=0',
                            'value': price,
                            'message': msg
                        })
                except Exception:
                    msg = f"Product {pid}: price invalid"
                    errors.append(msg)
                    details.append({
                        'dataset': 'products',
                        'record_id': pid,
                        'record_index': idx,
                        'field': 'price',
                        'issue': 'price_invalid',
                        'message': msg
                    })

            if 'rating_rate' in p:
                try:
                    rr = float(p.get('rating_rate'))
                    if rr < 0 or rr > 5:
                        issue = 'rating_rate>5' if rr > 5 else 'rating_rate<0'
                        msg = (
                            f"Product {pid}: rating_rate > 5" if rr > 5 else f"Product {pid}: rating_rate < 0"
                        )
                        errors.append(msg)
                        details.append({
                            'dataset': 'products',
                            'record_id': pid,
                            'record_index': idx,
                            'field': 'rating_rate',
                            'issue': issue,
                            'value': rr,
                            'message': msg
                        })
                except Exception:
                    msg = f"Product {pid}: rating_rate invalid"
                    errors.append(msg)
                    details.append({
                        'dataset': 'products',
                        'record_id': pid,
                        'record_index': idx,
                        'field': 'rating_rate',
                        'issue': 'rating_rate_invalid',
                        'message': msg
                    })

        seen: Dict[Any, int] = {}
        for idx, p in enumerate(products):
            pid = p.get('product_id') or p.get('id')
            if pid is None:
                continue
            if pid in seen:
                msg = f"products: Duplicado product_id {pid}"
                errors.append(msg)
                details.append({
                    'dataset': 'products',
                    'record_id': pid,
                    'record_index': idx,
                    'issue': 'duplicate',
                    'duplicate_of': seen[pid],
                    'message': msg
                })
            else:
                seen[pid] = idx

        return errors, details

    def _validate_sales(self, sales: List[Dict]) -> Tuple[List[str], List[Dict[str, Any]]]:
        errors: List[str] = []
        details: List[Dict[str, Any]] = []

        for idx, s in enumerate(sales):
            quantity = s.get('quantity')
            if quantity is None:
                msg = f"sales {idx}: quantity faltante"
                errors.append(msg)
                details.append({
                    'dataset': 'sales',
                    'record_index': idx,
                    'cart_id': s.get('cart_id'),
                    'product_id': s.get('product_id'),
                    'field': 'quantity',
                    'issue': 'missing',
                    'message': msg
                })
            else:
                try:
                    if int(quantity) <= 0:
                        msg = f"sales {idx}: quantity <= 0"
                        errors.append(msg)
                        details.append({
                            'dataset': 'sales',
                            'record_index': idx,
                            'cart_id': s.get('cart_id'),
                            'product_id': s.get('product_id'),
                            'field': 'quantity',
                            'issue': 'quantity<=0',
                            'value': quantity,
                            'message': msg
                        })
                except Exception:
                    msg = f"sales {idx}: quantity inválida"
                    errors.append(msg)
                    details.append({
                        'dataset': 'sales',
                        'record_index': idx,
                        'cart_id': s.get('cart_id'),
                        'product_id': s.get('product_id'),
                        'field': 'quantity',
                        'issue': 'invalid',
                        'message': msg
                    })

            if s.get('unit_price') is None and s.get('total_amount') is None:
                msg = f"sales {idx}: price/total faltante"
                errors.append(msg)
                details.append({
                    'dataset': 'sales',
                    'record_index': idx,
                    'cart_id': s.get('cart_id'),
                    'product_id': s.get('product_id'),
                    'field': 'unit_price',
                    'issue': 'missing',
                    'message': msg
                })

        seen: Dict[Tuple[Any, Any], int] = {}
        for idx, s in enumerate(sales):
            key = (s.get('cart_id'), s.get('product_id'))
            if key in seen:
                msg = f"sales: Duplicado cart_id/product_id {key}"
                errors.append(msg)
                details.append({
                    'dataset': 'sales',
                    'record_index': idx,
                    'cart_id': s.get('cart_id'),
                    'product_id': s.get('product_id'),
                    'issue': 'duplicate',
                    'duplicate_of': seen[key],
                    'message': msg
                })
            else:
                seen[key] = idx

        return errors, details

    def _validate_users(self, users: List[Dict]) -> Tuple[List[str], List[Dict[str, Any]]]:
        errors: List[str] = []
        details: List[Dict[str, Any]] = []

        for idx, user in enumerate(users):
            uid = user.get('user_id') or user.get('id')
            if uid is None:
                msg = f"users {idx}: user_id faltante"
                errors.append(msg)
                details.append({
                    'dataset': 'users',
                    'record_index': idx,
                    'record_id': uid,
                    'field': 'user_id',
                    'issue': 'missing',
                    'message': msg
                })
            if not user.get('email'):
                msg = f"users {idx}: email faltante"
                errors.append(msg)
                details.append({
                    'dataset': 'users',
                    'record_index': idx,
                    'record_id': uid,
                    'field': 'email',
                    'issue': 'missing',
                    'message': msg
                })

        seen: Dict[Any, int] = {}
        for idx, user in enumerate(users):
            uid = user.get('user_id') or user.get('id')
            if uid in seen and uid is not None:
                msg = f"users: Duplicado user_id {uid}"
                errors.append(msg)
                details.append({
                    'dataset': 'users',
                    'record_index': idx,
                    'record_id': uid,
                    'issue': 'duplicate',
                    'duplicate_of': seen[uid],
                    'message': msg
                })
            else:
                seen[uid] = idx

        return errors, details

    def _validate_completeness(self, data_type: str, data: List[Dict]) -> Tuple[List[str], List[Dict[str, Any]]]:
        errors: List[str] = []
        details: List[Dict[str, Any]] = []
        critical = self.rules.get(data_type, {}).get('critical_fields', [])
        if critical:
            for idx, rec in enumerate(data):
                for fld in critical:
                    if rec.get(fld) in (None, '', []):
                        msg = f"{data_type} {idx}: Campo crítico '{fld}' vacío"
                        errors.append(msg)
                        details.append({
                            'dataset': data_type,
                            'record_index': idx,
                            'field': fld,
                            'issue': 'missing',
                            'message': msg
                        })
        return errors, details

    def _validate_uniqueness(self, data_type: str, data: List[Dict]) -> List[str]:
        errors: List[str] = []
        key_fields = self.rules.get(data_type, {}).get('unique_keys', [])
        if not key_fields:
            return errors
        seen = set()
        for i, rec in enumerate(data):
            key = tuple(rec.get(f) for f in key_fields)
            if key in seen:
                errors.append(f"{data_type} {i}: duplicado en keys {key_fields} -> {key}")
            seen.add(key)
        return errors

    def _validate_referential_integrity(
        self,
        sales: List[Dict],
        products: List[Dict],
        users: List[Dict]
    ) -> Tuple[List[str], List[Dict[str, Any]]]:
        self.logger.info("[DQ] referential: verificando integridad referencial sales -> (products, users)")
        errors: List[str] = []
        details: List[Dict[str, Any]] = []

        valid_product_ids = {
            p.get('product_id') or p.get('id') for p in products
            if p.get('product_id') is not None or p.get('id') is not None
        }
        valid_user_ids = {
            u.get('user_id') or u.get('id') for u in users
            if u.get('user_id') is not None or u.get('id') is not None
        }

        for idx, sale in enumerate(sales):
            pid = sale.get('product_id')
            uid = sale.get('user_id')

            if pid is not None and pid not in valid_product_ids:
                msg = f"Producto {pid} no existe"
                errors.append(msg)
                details.append({
                    'dataset': 'sales',
                    'record_index': idx,
                    'cart_id': sale.get('cart_id'),
                    'product_id': pid,
                    'issue': 'foreign_key_product',
                    'message': msg
                })

            if uid is not None and uid not in valid_user_ids:
                msg = f"Usuario {uid} no existe"
                errors.append(msg)
                details.append({
                    'dataset': 'sales',
                    'record_index': idx,
                    'cart_id': sale.get('cart_id'),
                    'product_id': sale.get('product_id'),
                    'user_id': uid,
                    'issue': 'foreign_key_user',
                    'message': msg
                })

        self.logger.info(f"[DQ] referential: inconsistencias encontradas = {len(errors)}")
        return errors, details

    def validate_full_dataset(self, transformed_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        self.logger.info(
            "[DQ] Iniciando validaciones de calidad de datos (completitud, rangos, duplicados, integridad referencial)"
        )
        errors: List[str] = []
        details: List[Dict[str, Any]] = []
        records = 0

        for dataset, values in transformed_data.items():
            res = self.validate_data(dataset, values)
            errors.extend(res['errors'])
            details.extend(res.get('details', []))
            records += res['records_checked']

        if {'sales', 'products', 'users'}.issubset(transformed_data.keys()):
            self.logger.info("[DQ] Ejecutando validacion de integridad referencial entre sales y dimensiones")
            ref_errors, ref_details = self._validate_referential_integrity(
                transformed_data['sales'],
                transformed_data['products'],
                transformed_data['users']
            )
            errors.extend(ref_errors)
            details.extend(ref_details)

        result = {
            'is_valid': len(errors) == 0,
            'errors': errors,
            'records_checked': records,
            'errors_found': len(errors),
            'error_details': details
        }
        self.logger.info(f"[DQ] Finalizado. Registros chequeados: {records}. Errores: {len(errors)}")
        return result

    def generate_dq_report(self, validation_results: Dict[str, Any]) -> str:
        lines = [
            f"Data Quality Report - Valid: {validation_results.get('is_valid')}",
            f"Records checked: {validation_results.get('records_checked')}",
            f"Errors found: {validation_results.get('errors_found', len(validation_results.get('errors', [])))}"
        ]
        for err in validation_results.get('errors', []):
            lines.append(f"- {err}")
        return "\n".join(lines)
