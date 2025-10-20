#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador prolijo de diagrama star-schema para la Parte 1 (FakeStore).
Soporta salida .png/.jpg, guarda .dot si 'dot' no está en PATH y muestra comando
para generar la imagen manualmente.
"""
import os
import sys
import argparse
import shutil
from graphviz import Digraph

def build_graph():
    dot = Digraph(comment='Star Schema Parte1')
    # Cambiar layout a 'dot' para disposición radial/jerárquica
    dot.attr(layout='dot', size='10,10')
    dot.attr('node', fontname='Helvetica', fontsize='10')
    dot.attr('graph', overlap='false', splines='polyline')
    
    # Crear subgrafo para centrar FactSales
    with dot.subgraph(name='cluster_0') as c:
        c.attr(style='invis')  # Subgrafo invisible
        # Hecho en el centro
        c.node('FactSales', 
               label="{FactSales|sales_key PK\lproduct_key FK\luser_key FK\lgeo_key FK\ldate_key FK\lquantity\lunit_price\ltotal_amount\l}", 
               shape='record', 
               style='filled', 
               fillcolor='lightgrey')

    # Dimensiones alrededor
    dot.node('DimProduct', 
             label="{DimProduct|product_key PK\lproduct_id\ltitle\lcategory\lprice\lrating_rate\lrating_count\l}", 
             shape='record')
    dot.node('DimUser', 
             label="{DimUser|user_key PK\luser_id\lname_first\lname_last\lemail\lusername\lphone\l}", 
             shape='record')
    dot.node('DimGeography', 
             label="{DimGeography|geo_key PK\lcity\lstreet\lzipcode\llat\llng\l}", 
             shape='record')
    dot.node('DimDate', 
             label="{DimDate|date_key PK\ldate\lday\lmonth\lyear\nquarter\liso_week\l}", 
             shape='record')

    # Relaciones desde el centro hacia las dimensiones
    dot.edge('FactSales', 'DimProduct', arrowhead='none', label='product_key → product_key', fontsize='9')
    dot.edge('FactSales', 'DimUser', arrowhead='none', label='user_key → user_key', fontsize='9')
    dot.edge('FactSales', 'DimGeography', arrowhead='none', label='geo_key → geo_key', fontsize='9')
    dot.edge('FactSales', 'DimDate', arrowhead='none', label='date_key → date_key', fontsize='9')

    return dot

def get_format_from_ext(ext):
    ext = ext.lower().lstrip('.')
    if ext in ('jpg', 'jpeg'):
        return 'jpg'
    if ext == 'png' or ext == '':
        return 'png'
    return ext

def generate_der(output_path: str = "der_part1.png"):
    base, ext = os.path.splitext(output_path)
    fmt = get_format_from_ext(ext)
    if not base:
        base = "der_part1"

    dot = build_graph()
    dot.format = fmt

    # Si dot no está en PATH, guardar .dot y dar instrucciones
    if shutil.which("dot") is None:
        dot_file = base + ".dot"
        dot.save(dot_file)
        print(f"[WARN] 'dot' no encontrado en PATH. Se guardó: {dot_file}")
        print(f"Para generar la imagen ejecuta en PowerShell (en la carpeta del .dot):")
        print(f"  dot -T{fmt} {os.path.basename(dot_file)} -o {os.path.basename(base)}.{fmt}")
        print("Instalar Graphviz en Windows: choco install graphviz  (o descargar desde https://graphviz.org/download/ y añadir '.../Graphviz/bin' al PATH)")
        return

    # Renderizar (Graphviz añadirá la extensión)
    try:
        out_path = dot.render(filename=base, cleanup=True)
        print(f"[OK] Diagrama generado: {out_path}")
    except Exception as e:
        print("[ERROR] Falló al renderizar con Graphviz:", str(e))
        # Guardar .dot para uso manual
        dot_file = base + ".dot"
        dot.save(dot_file)
        print(f"[INFO] Se guardó el DOT en: {dot_file}")

def parse_args(argv):
    p = argparse.ArgumentParser(description="Genera DER (star-schema) para FakeStore - salida .png/.jpg")
    p.add_argument("output", nargs="?", default="der_part1.png", help="Ruta de salida (ej: der_part1.png o der_part1.jpg)")
    return p.parse_args(argv)

if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    generate_der(args.output)
