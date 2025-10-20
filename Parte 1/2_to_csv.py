#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FakeStore Data Exploration & Normalization
------------------------------------------
Usa pandas para leer los tres JSON de la API:
  - products.json
  - carts.json
  - users.json

Realiza:
  1. Lectura y aplanado
  2. Limpieza de columnas anidadas
  3. Normalización (dimensiones preliminares)
  4. Muestras y verificaciones
"""

import pandas as pd
import json

# =========================
# 1. Leer los archivos
# =========================
df_products = pd.read_json("data_raw/products.json")
df_carts = pd.read_json("data_raw/carts.json")
df_users = pd.read_json("data_raw/users.json")

print("\n=== Dimensiones iniciales ===")
print(f"Products: {df_products.shape}")
print(f"Carts:    {df_carts.shape}")
print(f"Users:    {df_users.shape}")

# =========================
# 2. Aplanar estructuras anidadas
# =========================
# Products -> rating
rating_df = pd.json_normalize(df_products['rating'])
df_products = df_products.drop(columns=['rating']).join(rating_df)
df_products.rename(columns={'rate': 'rating_rate', 'count': 'rating_count'}, inplace=True)

# Users -> address & name
address_df = pd.json_normalize(df_users['address'])
geoloc_df = pd.json_normalize(df_users['address'].apply(lambda x: x['geolocation']))
name_df = pd.json_normalize(df_users['name'])

df_users = (
    df_users
    .drop(columns=['address', 'name'])
    .join(address_df.add_prefix('addr_'))
    .join(geoloc_df.add_prefix('geo_'))
    .join(name_df.add_prefix('name_'))
)

# Carts -> products (expandir items)
cart_items = []
for _, row in df_carts.iterrows():
    cart_id = row['id']
    user_id = row['userId']
    date = row['date']
    for item in row['products']:
        cart_items.append({
            "cart_id": cart_id,
            "user_id": user_id,
            "date": date,
            "product_id": item['productId'],
            "quantity": item['quantity']
        })
df_cart_items = pd.DataFrame(cart_items)

# =========================
# 3. Limpieza y tipos
# =========================
df_products['category'] = df_products['category'].str.title()
df_products['price'] = pd.to_numeric(df_products['price'], errors='coerce')
df_products['rating_rate'] = pd.to_numeric(df_products['rating_rate'], errors='coerce')
df_cart_items['date'] = pd.to_datetime(df_cart_items['date'])

# =========================
# 4. Métricas derivadas
# =========================
df_cart_items = df_cart_items.merge(
    df_products[['id', 'price']], left_on='product_id', right_on='id', how='left'
)
df_cart_items['total_amount'] = df_cart_items['quantity'] * df_cart_items['price']

# =========================
# 5. Previsualización
# =========================
print("\n=== Productos (5 filas) ===")
print(df_products.head())

print("\n=== Usuarios (5 filas) ===")
print(df_users.head())

print("\n=== Cart Items (5 filas) ===")
print(df_cart_items.head())

# =========================
# 6. Guardar intermedios
# =========================
import os

# Create data_clean directory if it doesn't exist
os.makedirs("data_clean", exist_ok=True)

df_products.to_csv("data_clean/products_clean.csv", index=False)
df_users.to_csv("data_clean/users_clean.csv", index=False)
df_cart_items.to_csv("data_clean/cart_items.csv", index=False)

print("\n[OK] Archivos limpios guardados en 'data_clean/'")
