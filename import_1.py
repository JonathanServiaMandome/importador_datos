# -*- coding: utf-8 -*-

from import_functions import *

if __name__ == '__main__':
    levels = [1,2,3]
    for level in levels:
        data_base = Database(level)
        if level == 1:
            data_base.reset()

            insert_empresa(data_base)
            insert_ivacuota(data_base)
            insert_modulos(data_base)
            insert_menus(data_base)
            insert_tipos_registros(data_base)
            insert_divisas(data_base)
            insert_idiomas(data_base)
            insert_paises(data_base)
            insert_provincias(data_base)
            insert_sub_tipos_registros(data_base)
            insert_clasificacionclientes(data_base)
            insert_caracteristica(data_base)
            insert_opciones_tipos(data_base)
            insert_fechas(data_base)
            insert_textos(data_base)
            documentos = insert_normativa(data_base)
            insert_checklists(data_base)
            insert_tipos_acciones(data_base)
            insert_clasificacionarticulos(data_base)
            insert_zonas(data_base)
            insert_sociedades(data_base)
            insert_tarifas(data_base)
            insert_rutas(data_base)
            insert_marcas(data_base)
            n = insert_tipos(data_base)
            insert_series(data_base)
            insert_tipos_contratos(n, data_base)
            insert_codigo_postal(data_base)
            insert_administradores(data_base)
            insert_almacenes(data_base)
            insert_bancos(data_base)
            insert_departamentos(data_base)
            insert_capitulos(data_base)
            insert_transportistas(data_base)
            insert_certificados(data_base)
            insert_formas_pago(data_base)
            insert_personal(data_base)
            insert_tipos_partes(data_base)
            insertar_documentos_normativa(documentos, data_base)
            insert_proveedores(data_base)
            insert_articulos(data_base)
            delegaciones_defecto = insert_clientes(data_base)
            insert_delegaciones(delegaciones_defecto, data_base)
        elif level == 2:

            insert_pprecios(data_base)
            insert_pedidoproveedor(data_base)
            insert_alb_compra(data_base)
            insert_facturasc(data_base)
            insert_pagos(data_base)
            insert_remesaspagos(data_base)
        elif level == 3:
            insert_presupuestos(data_base)
            insert_pedidocliente(data_base)
            insert_albaranventa(data_base)
        elif level == 4:
            insert_facturasv(data_base)
        data_base.close()
