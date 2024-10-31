# -*- coding: utf-8 -*-

from functions import *
from import_functions import sanitize, Database, log, get_data, dict_to_sql, parser_date, i_fechas, i_caracteristicas, \
    PATH, i_relacion

reset = True

def insert_nseries():
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'elemento', 'nseries', data_base.dc_db)
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('elemento')
        data_base.delete_where('relacionregistro', 'registro_padre=%d' % n_registro)
        data_base.delete_where('caracteristicaregistro', 'registro=%d' % n_registro)
        data_base.delete_where('fecharegistro', 'registro=%d' % n_registro)

    ruta_c = PATH + 'nseries_caracteristicas'
    rgs_caracteristicas = bkopen(ruta_c, huf=0)

    relacionados = list()

    n=0
    c=0
    for idx in idxs:
        if not idx.strip():
            continue

        (numero_de_serie, articulo, albaran_de_venta_, cliente, delegacion, fecha_proxima_certif, fecha_fabricacion_va,
         fecha_prevista_baja, fecha_ultima_carga_v, anos_de_vida_util_va, fecha_ultimo_timbrad,
         fecha_proximo_timbra, baja, fecha_de_la_baja, motivo_de_la_baja, localizacion, fecha_ultima_revisio,
         fecha_proxima_revisi, administrador_de_fin, observaciones, contrato, vendedor_o_comercial, cliente_,
         fabricante_variable, eficacia_del_extinto, tipo_extintor_variab, modelo_variable_inte, marca,
         ref_interna_matricul, periodicidad_de_reti, ref_anterior_variabl, peso_en_kg_variable,
         tara_en_kg_variable, volumen_en_litros_va, parte_, variable_internaper, tipo_periodicidad_re,
         tipo_periodicidad_re, tipo_periodicidad_vi, id_linea_contrato_va, dia_fijo_para_calcul,
         origen_del_numero_de, fecha_certificacion, albaran_de_compra_, almacen, caracteristicas_, tipo_periodicidad_ce,
         periodicidad_certifi, codigo_de_barras, tipo_de_fecha_, numero_pregunta_, codigo_checklists, observaciones_intern)  = file_rg[idx]

        idx = sanitize(idx)

        articulo = data_base.dc_db['articulos'].get(articulo)
        cliente = data_base.dc_db['clientes'].get(cliente)
        delegacion = data_base.dc_db['delegaciones'].get(delegacion)
        marca = data_base.dc_db['marcas'].get(marca)
        if articulo is None:
            c+=1
            continue
        i_fechas(data_base, tipo_de_fecha_, n, n_registro, relacionados, data_base.dc_db)
        i_caracteristicas(data_base, caracteristicas_, n, n_registro, relacionados, rgs_caracteristicas, data_base.dc_db)
        if codigo_checklists:
            i_relacion(data_base, [codigo_checklists], 'checklist', n, n_registro, relacionados, data_base.dc_db, 'checklists')
        if almacen:
            raise ValueError(almacen)
        if contrato:
            data_base.dc_parser['relaciones_faltan'].append(['relacionregistro',n, n_registro, 'contratos', contrato])

        """ 
        
            u'': sanitize(albaran_de_venta_),
            u'': sanitize(numero_pregunta_),
            u'': sanitize(parte_),
            u'': sanitize(albaran_de_compra_),
            u'': sanitize(cliente_),
        """# TODO
        n+=1
        if n % 2500 == 0.:
            log(idx, len(idxs), n)
        data_base.dc_parser[table_erp][idx] = n

        dc = {
            u'id': sanitize(n),
            u'empresa_id': empresa_id,
            u'numero_serie': sanitize(numero_de_serie),
            u'articulo_id': sanitize(articulo),
            u'cliente_id': sanitize(cliente),
            u'delegacion_id': sanitize(delegacion),
            u'baja': sanitize(baja=='S'),
            u'fecha_baja': sanitize(parser_date(fecha_de_la_baja)),
            u'motivo_baja': sanitize(motivo_de_la_baja),
            u'ubicacion': sanitize(localizacion),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'marca_id': sanitize(marca),
            u'dia_fijo_fechas': sanitize(dia_fijo_para_calcul),
            u'codigo_barras': sanitize(codigo_de_barras),
            u'observaciones_internas': sanitize(stripRtf(observaciones_intern)),
        }

        dict_to_sql(data_base, dc, table_db)

    log('Relacionados ' + table_db, len(relacionados))
    i = 0

    for sql_r in relacionados:
        i += 1
        if i % 50000 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)


def insert_descompuestos():
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'articulocompuesto', 'descompuestos', data_base.dc_db)
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('articulocompuesto')
        data_base.delete_all('articulocompuestocosteindirecto')
        #data_base.delete_where('checklistsecundarioregistros', 'registro=%d' % n_registro)

    lineas_ls = list()
    relacionados = list()

    n=0
    c=0
    for idx in idxs:
        if not idx.strip():
            continue

        (cdar, nombre, unidades_padre, actualiza_precio_de_venta, actualiza_precio_de_coste, codigo_documento_padre,
         archivo_documento_padre, descompuesto_original, lineas, costes_ls, observaciones)  = file_rg[idx]

        idx = sanitize(idx)

        id_registro = 1 # codigo_documento_padre TODO
        registro = 1#data_base.dc_db['tiporegistros'].get(archivo_documento_padre) TODO
        articulo = data_base.dc_db['articulos'].get(cdar)
        if articulo is None:
            c+=1
            print(cdar)
            continue

        n+=1
        if n % 500 == 0.:
            log(idx, len(idxs), n)
        data_base.dc_parser[table_erp][idx] = n

        dc = {
            'id': sanitize(n),
            'codigo': sanitize(idx),
            'empresa_id': sanitize(1),
            'articulo_padre_id': sanitize(articulo),
            'nombre': sanitize(nombre),
            'unidades_padre': sanitize(unidades_padre),
            'actu_pventa': sanitize(actualiza_precio_de_venta=='S'),
            'actu_pcoste': sanitize(actualiza_precio_de_coste=='S'),
            'id_registro': sanitize(id_registro),
            'registro': sanitize(registro),
            'observaciones': sanitize(observaciones)
        }

        dict_to_sql(data_base, dc, table_db)

    log('Relacionados ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)


if __name__ == '__main__':
    data_base = Database(3)
    empresa_id = '1'

    #insert_descompuestos()
    insert_nseries()

    data_base.close()
