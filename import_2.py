# -*- coding: utf-8 -*-

from functions import *
from import_functions import _join, parser_date, sanitize, Database, log, _direcciones, _contactos, _iban, _relacion, \
    _traducciones, _fechas, _caracteristicas, _checks, _modeloscertificado, _checksecundario

reset = True

def insert_proveedores():

    table_db = 'proveedores'
    table_erp = 'proveedores'
    n_registro = data_base.dc_db['tiporegistros'][table_db]
    if reset:
        data_base.delete_all(table_db)
        data_base.delete_where('direcciones', 'registro=%d' % n_registro)
        data_base.delete_where('contactos', 'registro=%d' % n_registro)
    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = list(file_rg.keys())
    idxs.sort()

    log(table_erp, len(idxs))

    n_proveedor=0
    relacionados = list()
    for idx in idxs:
        if not idx.strip():
            continue
        n_proveedor+=1
        if n_proveedor % 100 == 0.:
            log(idx, len(idxs), n_proveedor)
        data_base.dc_parser[table_erp][idx] = n_proveedor

        (nombre_fiscal,nombre_comercial,domicilio,poblacion,provincia,codigo_postal,telefono,fax,cif_dni,email,web,divisa,
         actividad_comercial,tipo,cuenta_cantable,cuenta_contable_efectos,bloqueado,observaciones,transportista,forma_pago,
         banco,sociedad,tipos_de_portes,tipo_facturacion_auto,descuento_general,descuento_pronto_pago,tipo_de_iva,
         pedir_a,cargos,fecha_ultima_compra,pais,cuenta_bases,tipo_compra,iban,bic_swift,primer_dia,segundo_dia,codigo_anterior,
         fecha_bloqueo,pedido_minimo_unidades,importe_pedido_minimo,motivo_bloqueo,contabilizado,integra_en_contabilidad,
         proveedor_de_facturacion,n2,n3,marca_serie,nima,cnae,autorizacion,tipo_de_autorizacion,zona,sub_zona,ruta,
         bloqueado_para_la_facturacion_automatica)  = file_rg[idx]

        _direcciones(data_base,[[domicilio, codigo_postal, poblacion, provincia, pais]], n_proveedor, n_registro, relacionados, data_base.dc_db, True, True)
        _contactos(data_base, cargos, n_proveedor, n_registro, relacionados)

        divisa = data_base.dc_db['divisas'].get(divisa)
        actividad = data_base.dc_db['actividades'].get(actividad_comercial)
        transportista = data_base.dc_db['transportistas'].get(transportista)
        forma_pago = data_base.dc_db['fpago'].get(forma_pago)
        if forma_pago is None:
            forma_pago = data_base.dc_db['fpago'].get('001')
        banco = data_base.dc_db['bancos'].get(banco)
        sociedad = data_base.dc_db['sociedades'].get(sociedad)
        zona = data_base.dc_db['zonas'].get(zona)
        sub_zona = data_base.dc_db['subzonas'].get(sub_zona)
        ruta = data_base.dc_db['rutas'].get(ruta)

        tipo_facturacion_auto = data_base.dc_db['opcionestipos'].get(tipo_facturacion_auto+'|tipo_facturacion_auto')
        if tipo_facturacion_auto is None:
            tipo_facturacion_auto = data_base.dc_db['opcionestipos'].get('T|tipo_facturacion_auto')

        tipo_de_iva = data_base.dc_db['opcionestipos'].get(str(tipo_de_iva)+'|tipo_iva_proveedor')
        tipo = data_base.dc_db['opcionestipos'].get(tipo+'|tipo_proveedor')
        marca_serie = None# data_base.dc_db['marcasriesgo'].get(marca_serie)

        try:
            obs = stripRtf(observaciones)
        except Exception as e:
            obs = None


        dc = {
            'id': sanitize(n_proveedor),
            u'empresa_id': sanitize(empresa_id),
            u'divisa_id': sanitize(divisa),
            u'actividad_id': sanitize(actividad),
            u'transportista_id': sanitize(transportista),
            u'metodo_pago_id': sanitize(forma_pago),
            u'banco_id': sanitize(banco),
            u'tipos_portes': sanitize(tipos_de_portes=='P'),
            u'tipo_facturacion_id': sanitize(tipo_facturacion_auto),
            u'tipo_id': sanitize(tipo),
            u'sociedad_id': sanitize(sociedad),
            u'marca_serie_id': sanitize(marca_serie),
            u'zona_id': sanitize(zona),
            u'sub_zona_id': sanitize(sub_zona),
            u'ruta_id': sanitize(ruta),
            #u'proveedor_facturacion': sanitize(),
            u'codigo': sanitize(idx),
            u'nombre_fiscal': sanitize(nombre_fiscal),
            u'nombre_comercial': sanitize(nombre_comercial),
            u'telefono': sanitize(telefono),
            u'fax': sanitize(fax),
            u'cif': sanitize(cif_dni),
            u'email': sanitize(email),
            u'web': sanitize(web),
            u'cuenta_contable': sanitize(cuenta_cantable),
            u'cuenta_contable_efectos': sanitize(cuenta_contable_efectos),
            u'bloqueado': sanitize(bloqueado=='S'),
            u'fecha_bloqueo': sanitize(fecha_bloqueo),
            u'motivo_bloqueo': sanitize(motivo_bloqueo),
            u'observaciones': sanitize(obs),
            u'descuento_general': sanitize(descuento_general),
            u'descuento_ppago': sanitize(descuento_pronto_pago),
            u'tipo_iva_id': sanitize(tipo_de_iva),
            u'pedir_a': sanitize(pedir_a),
            u'fecha_ultima_compra': sanitize(parser_date(fecha_ultima_compra)),
            u'cuenta_bases': sanitize(cuenta_bases),
            u'tipo_compra': sanitize(tipo_compra),
            u'iban': sanitize(iban),
            u'primer_dia_pago ': sanitize(primer_dia),
            u'segundo_dia_pago ': sanitize(segundo_dia),
            u'unidades_minimas': sanitize(pedido_minimo_unidades),
            u'importe_minimo': sanitize(importe_pedido_minimo),
            u'contabilizado': sanitize(contabilizado=='S'),
            u'integra_contabilidad': sanitize(integra_en_contabilidad=='S'),
            u'bloqueado_facturacion': sanitize(bloqueado_para_la_facturacion_automatica=='S'),
            u'codigo_anterior': sanitize(codigo_anterior)

        }
        columns = []
        values = []
        ke = list(dc.keys())
        ke.sort()
        for k in ke:
            v = dc[k]
            if v is None:
                continue
            columns.append(k)
            values.append(v)

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    log('Direcciones y contactos ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)

def insert_clientes():

    delegaciones_defecto = {}
    table_db = 'clientes'
    table_erp = 'clientes'
    n_registro = data_base.dc_db['tiporegistros'][table_db]
    if reset:
        data_base.delete_all(table_db)
        data_base.delete_where('direcciones', 'registro=%d' % n_registro)
        data_base.delete_where('contactos', 'registro=%d' % n_registro)
        data_base.delete_where('ibanregistros', 'registro=%d' % n_registro)
        data_base.delete_where('modeloscertificadoregistros', 'registro=%d' % n_registro)
    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = list(file_rg.keys())
    idxs.sort()

    log(table_erp, len(idxs))

    n_cliente=0
    relacionados = list()
    for idx in idxs:
        if not idx.strip():
            continue
        n_cliente+=1
        if n_cliente % 100 == 0.:
            log(idx, len(idxs), n_cliente)
        data_base.dc_parser[table_erp][idx] = n_cliente

        (nombre_fiscal, nombre_comercial, cif_dni, codigo_anterior, direccion_, telefono_, sector_actividad, sub_sector_actividad,
         actividad, transportista, forma_de_pago, vendedor_por_defecto, cobrador, administrador_de_fincas, tarifa_precios,
         zona, moroso, delegacion_por_defecto, cuenta_contable, cuenta_contable_efectos, limite_credito, credito_actual,
         descuento_general, descuento_pronto_pago, comision_fija, fecha_creacion, fecha_ultima_venta, tipo_iva,
         tipo_impresion_factura, serie_para_documentos, n_copias_factura, primer_dia, segundo_dia, boqueado,
         agrupar_por_del_fac_contratos, facturar_directamente_contratos, cabecera_de_presupuestos, tipo_facturacion_automatica,
         valorar_albaranes, oficina_contable_factura_e, organo_gestor_factura_e, unidad_tramitadora_factura_e, tipo_factura_e,
         articulo_libre, ofertas_venta, iban_, familia_descuento, observaciones, observaciones_moroso, tipo_de_adeudo,
         numero_mandato, fecha_firma_mandato, marca_riesgo, riesgo_solicitado, fecha_solicitud_riesgo, riesgo_externo,
         fecha_de_bloqueo, motivo_del_bloqueo, sub_zona, receptor_mercancia_edi, comprobador_edi, departamento_edi,
         receptor_factura_edi, pagador_edi, contabilizado, no_integra_contabilidad, bloquear_en_facturacion_automatica,
         nada, periodicidad_facturacion, periodicidad_revisiones, tipo_periodicidad_revisiones, tecnico_s_por_defecto,
         traspasado_variable_interna, cabecera_facturas, categoria, latitud_variable_interna, longitud_variable_interna,
         cert_, divisa, sociedad, idioma_por_defecto, proveedor_vinculado, codigo_nima, codigo_cnae, nada, nada, ruta,
         periodicidad_envio_liquidaciones)  = file_rg[idx]

        delegaciones_defecto[idx] = delegacion_por_defecto

        for lnd in direccion_:
            _direcciones(data_base,[[lnd[0], lnd[1], lnd[2], lnd[3], lnd[4]]], n_cliente, n_registro, relacionados, data_base.dc_db, True, True)
        for ln in telefono_:
            teleo, nom, cif, ema, fax, defe = ln[:6]
            _contactos(data_base,[[nom, None, teleo, ema, defe, cif, fax]], n_cliente, n_registro, relacionados)
        _iban(data_base, iban_, n_cliente, n_registro, relacionados)
        _relacion(data_base, vendedor_por_defecto, 'clientes', n_cliente, n_registro, relacionados, data_base.dc_db)
        _modeloscertificado(data_base, cert_, n_cliente, n_registro, relacionados, data_base.dc_db)

        if ofertas_venta:
            raise ValueError('1 '+str(ofertas_venta))
        if vendedor_por_defecto:
            raise ValueError('2 '+str(vendedor_por_defecto))
        if tecnico_s_por_defecto==['']:
            tecnico_s_por_defecto=[]
        if tecnico_s_por_defecto:
            raise ValueError('3 '+str(tecnico_s_por_defecto))

        divisa = data_base.dc_db['divisas'].get(divisa)
        categoria = data_base.dc_db['categorias_clientes'].get(categoria)
        actividad = data_base.dc_db['actividades'].get(actividad)
        forma_de_pago = data_base.dc_db['fpago'].get(forma_de_pago)
        if forma_de_pago is None:
            forma_de_pago = data_base.dc_db['fpago'].get('001')

        sociedad = data_base.dc_db['sociedades'].get(sociedad)
        zona = data_base.dc_db['zonas'].get(zona)
        sub_zona = data_base.dc_db['subzonas'].get(sub_zona)
        ruta = data_base.dc_db['rutas'].get(ruta)
        sector_actividad = data_base.dc_db['sectores'].get(sector_actividad)
        sub_sector_actividad = data_base.dc_db['subsectores'].get(sub_sector_actividad)
        administrador_de_fincas = data_base.dc_db['administradores'].get(administrador_de_fincas)
        tarifa_precios = data_base.dc_db['tarifas'].get(tarifa_precios)
        if tarifa_precios is None:
            tarifa_precios = data_base.dc_db['tarifas'].get('01')
        idioma_por_defecto = data_base.dc_db['idiomas'].get(idioma_por_defecto)
        serie_para_documentos = data_base.dc_db['series'].get(serie_para_documentos)
        transportista = data_base.dc_db['transportistas'].get(transportista)


        tipo_iva = data_base.dc_db['opcionestipos'].get(str(tipo_iva)+'|tipo_iva_cliente')
        tipo_facturacion_automatica = data_base.dc_db['opcionestipos'].get(str(tipo_facturacion_automatica)+'|tipo_facturacion_auto')
        if tipo_facturacion_automatica is None:
            tipo_facturacion_automatica = data_base.dc_db['opcionestipos'].get('T|tipo_facturacion_auto')
        marca_riesgo = None# data_base.dc_db['marcasriesgo'].get(marca_serie)


        try:
            observaciones = stripRtf(observaciones)
        except Exception as e:
            observaciones = None


        dc = {
            'empresa_id': sanitize(empresa_id),
            'id': sanitize(n_cliente),
            'codigo': sanitize(idx),
            'factura_directa': sanitize(False),
            'actividad_id': sanitize(actividad),
            'administrador_id': sanitize(administrador_de_fincas),
            'agrupar_delegacion': sanitize(agrupar_por_del_fac_contratos),
            'bloquear_factu_auto': sanitize(bloquear_en_facturacion_automatica != 'S'),
            'boqueado': sanitize(boqueado=='S'),
            'categoria_id': sanitize(categoria),
            'cif': sanitize(cif_dni),
            'codigo_anterior': sanitize(codigo_anterior),
            'comision': sanitize(comision_fija),
            'contabilizado': sanitize(contabilizado=='S'),
            'credito_actual': sanitize(credito_actual),
            'cuenta_contable': sanitize(cuenta_contable),
            'descuento_general': sanitize(descuento_general),
            'descuento_ppago': sanitize(descuento_pronto_pago),
            'divisa_id': sanitize(divisa),
            'fecha_creacion': sanitize(parser_date(fecha_creacion)),
            'fecha_bloqueo': sanitize(parser_date(fecha_de_bloqueo)),
            'fecha_firma_mandato': sanitize(parser_date(fecha_firma_mandato)),
            'fecha_solicitud_riesgo': sanitize(parser_date(fecha_solicitud_riesgo)),
            'fecha_ultima_venta': sanitize(parser_date(fecha_ultima_venta)),
            'forma_cobro_id': sanitize(forma_de_pago),
            'idioma_id': sanitize(idioma_por_defecto),
            'limite_credito': sanitize(limite_credito),
            'marca_riesgo': sanitize(marca_riesgo),
            'motivo_bloqueo': sanitize(motivo_del_bloqueo),
            'n_copias_factura': sanitize(n_copias_factura),
            'integra_contabilidad': sanitize(no_integra_contabilidad != 'S'),
            'nombre_comercial': sanitize(nombre_comercial),
            'nombre_fiscal': sanitize(nombre_fiscal),
            'numero_mandato': sanitize(numero_mandato),
            'observaciones': sanitize(observaciones),
            'periodicidad_factura': sanitize(periodicidad_facturacion),
            'periodicidad_revisiones': sanitize(periodicidad_revisiones),
            'primer_dia_cobro': sanitize(primer_dia),
            'riesgo_externo': sanitize(riesgo_externo),
            'riesgo_solicitado': sanitize(riesgo_solicitado),
            'ruta_id': sanitize(ruta),
            'sector_id': sanitize(sector_actividad),
            'segundo_dia_cobro': sanitize(segundo_dia),
            'serie_id': sanitize(serie_para_documentos),
            'sociedad_id': sanitize(sociedad),
            'sub_sector_id': sanitize(sub_sector_actividad),
            'sub_zona_id': sanitize(sub_zona),
            'tarifa_precios_id': sanitize(tarifa_precios),
            'posible': sanitize(False),
            'tipo_adeudo': sanitize(tipo_de_adeudo),
            'tipo_factu_automatica_id': sanitize(tipo_facturacion_automatica),
            'tipo_imp_facturas': sanitize(tipo_impresion_factura),
            'tipo_iva_id': sanitize(tipo_iva),
            'tipo_per_revisiones': sanitize(tipo_periodicidad_revisiones),
            'transportista_id': sanitize(transportista),
            'valorar_albaranes': sanitize(valorar_albaranes=='S'),
            'zona_id': sanitize(zona),

        }
        columns = []
        values = []
        ke = list(dc.keys())
        ke.sort()
        for k in ke:
            v = dc[k]
            if v is None:
                continue
            columns.append(k)
            values.append(v)

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    log('Direcciones y contactos ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)

    return delegaciones_defecto

def insert_delegaciones(delegaciones_defecto):

    table_db = 'delegaciones'
    table_erp = 'delegaciones'
    n_registro = data_base.dc_db['tiporegistros'][table_db]
    if reset:
        data_base.delete_all(table_db)
        data_base.delete_where('direcciones', 'registro=%d' % n_registro)
        data_base.delete_where('contactos', 'registro=%d' % n_registro)
        data_base.delete_where('ibanregistros', 'registro=%d' % n_registro)
        data_base.delete_where('modeloscertificadoregistros', 'registro=%d' % n_registro)
    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = list(file_rg.keys())
    idxs.sort()

    log(table_erp, len(idxs))

    n_cliente=0
    relacionados = list()
    omitidas = 0
    for idx in idxs:
        if not idx.strip():
            continue
        n_cliente+=1
        if n_cliente % 100 == 0.:
            log(idx, len(idxs), n_cliente)

        data_base.dc_parser[table_erp][idx] = n_cliente

        (cli, numerodedelegacion, nombrecomercial, codigoanterior, tipo, bloqueado, fechadebloqueo, motivobloqueo,
         tipoautorizaciondegestionderesiduos, direccionfacturacion_ls, direccion_ls, sectordeactividad, subsectordeactividad,
         actividadcomercial, formadepago, vendedor_ls, zona, observaciones, observacionestestasat, moroso, libre, iban_ls,
         subzona, receptormercancia_edi, comprobador_edi, departamento_edi, receptorfactura_edi, pagador_edi, traspasado,
         nointegraencontabilidad, _1dia, _2dia, cuentacontable, cuentacontableefectos, telefono_ls, administradordefincas,
         transportista, imagenespecialparaimpresiones, tecnicospordefecto, ofertas_venta_ls, tarifa, latiutud, longitud,
         cobrador, modelos_ls, divisa, sociedad, idiomapordefecto, autorizaciongestionresiduos, codigonima, codigocnae,
         ruta, autorizacionproductor, tipodeautorizacionproductor, nimaproductor, horario, categoria)  = file_rg[idx]

        n_del = delegaciones_defecto.get(cli)
        if n_del is None:
            omitidas += 1
            continue
            raise ValueError('*** '+idx)
        cliente = data_base.dc_parser['clientes'].get(cli)
        if n_del is None:
            omitidas += 1
            continue
            raise ValueError('*** '+cli+ ' ' + idx)

        _direcciones(data_base,direccionfacturacion_ls, n_cliente, n_registro, relacionados, data_base.dc_db, True, False)
        _direcciones(data_base,direccion_ls, n_cliente, n_registro, relacionados, data_base.dc_db, False, True)

        for ln in telefono_ls:
            teleo, nom, cif, ema, fax, defe = ln[:6]
            _contactos(data_base,[[nom, None, teleo, ema, defe, cif, fax]], n_cliente, n_registro, relacionados)
        _iban(data_base, iban_ls, n_cliente, n_registro, relacionados)
        _relacion(data_base, vendedor_ls, 'delegaciones', n_cliente, n_registro, relacionados, data_base.dc_db)
        _modeloscertificado(data_base, modelos_ls, n_cliente, n_registro, relacionados, data_base.dc_db)

        if ofertas_venta_ls:
            raise ValueError('1 '+str(ofertas_venta_ls))

        divisa = data_base.dc_db['divisas'].get(divisa)
        categoria = data_base.dc_db['categorias_clientes'].get(categoria)
        actividadcomercial = data_base.dc_db['actividades'].get(actividadcomercial)
        formadepago = data_base.dc_db['fpago'].get(formadepago)


        sociedad = data_base.dc_db['sociedades'].get(sociedad)
        zona = data_base.dc_db['zonas'].get(zona)
        subzona = data_base.dc_db['subzonas'].get(subzona)
        ruta = data_base.dc_db['rutas'].get(ruta)
        sectordeactividad = data_base.dc_db['sectores'].get(sectordeactividad)
        subsectordeactividad = data_base.dc_db['subsectores'].get(subsectordeactividad)
        administradordefincas = data_base.dc_db['administradores'].get(administradordefincas)
        tarifa = data_base.dc_db['tarifas'].get(tarifa)
        idiomapordefecto = data_base.dc_db['idiomas'].get(idiomapordefecto)
        transportista = data_base.dc_db['transportistas'].get(transportista)
        tipo = data_base.dc_db['delegaciones_tipos'].get(tipo)

        try:
            obsi = stripRtf(observacionestestasat)
        except KeyError as e:
            obsi = None

        try:
            obs = stripRtf(observaciones)
        except KeyError as e:
            obs = None


        dc = {
            'empresa_id': sanitize(empresa_id),
            'id': sanitize(n_cliente),
            'codigo': sanitize(numerodedelegacion),
            'segundo_dia_cobro': sanitize(_2dia),
            'actividad_id': sanitize(actividadcomercial),
            'administrador_id': sanitize(administradordefincas),
            'bloqueado': sanitize(bloqueado=='S'),
            'categoria_id': sanitize(categoria),
            'cliente_id': sanitize(cliente),
            'codigo_anterior': sanitize(codigoanterior),
            'divisa_id': sanitize(divisa),
            'fecha_bloqueo': sanitize(parser_date(fechadebloqueo)),
            'forma_pago_id': sanitize(formadepago),
            'horario': sanitize(horario),
            'idioma_id': sanitize(idiomapordefecto),
            'latiutud': sanitize(latiutud),
            'longitud': sanitize(longitud),
            'motivo_bloqueo': sanitize(motivobloqueo),
            'nombre_comercial': sanitize(nombrecomercial),
            'observaciones': sanitize(obs),
            'observaciones_internas': sanitize(obsi),
            'primer_dia_cobro': sanitize(_1dia),
            'ruta_id': sanitize(ruta),
            'sector_id': sanitize(sectordeactividad),
            'sociedad_id': sanitize(sociedad),
            'sub_sector_id': sanitize(subsectordeactividad),
            'sub_zona_id': sanitize(subzona),
            'tarifa_id': sanitize(tarifa),
            'tipo_id': sanitize(tipo),
            'transportista': sanitize(transportista),
            'traspasado': sanitize(traspasado == 'S'),
            'zona_id': sanitize(zona),
            'defecto': sanitize(numerodedelegacion == n_del),

        }
        columns = []
        values = []
        ke = list(dc.keys())
        ke.sort()
        for k in ke:
            v = dc[k]
            if v is None:
                continue
            columns.append(k)
            values.append(v)

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    log('Direcciones y contactos ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)


def insert_articulos():
    def _proveedores(ls, na, rels):
        table_rel = 'articulosproveedores'
        for lnp in ls:
            proveedor_i, pcompra_ln, dto_ln, fec_u_compra_ln, alb_compra_id, referencia_ln, p_acordado_ln, defecto = lnp
            proveedor_id = data_base.dc_parser['proveedores'].get(proveedor_i)
            if proveedor_id is None:
                continue


            dc_lnp = {
                'empresa_id': '1',
                'articulo_id': sanitize(na),
                'proveedor_id': sanitize(proveedor_id),
                'precio_compra': sanitize(pcompra_ln),
                'descuento': sanitize(dto_ln),
                'fecha_ultima_compra': sanitize(parser_date(fec_u_compra_ln)),
                # 'albaran_compra': sanitize(alb_compra_id), #TODO
                'referencia': sanitize(referencia_ln),
                'precio_acordado': sanitize(p_acordado_ln),
                'defecto': sanitize(defecto == 'S'),
            }
            columns_ln = list()
            values_ln = list()
            kep = list(dc_lnp.keys())
            kep.sort()
            for kp in kep:
                vp = dc_lnp[kp]
                if vp is None:
                    continue
                columns_ln.append(kp)
                values_ln.append(vp)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))
            rels.append(sql_ln)

    def _descompuestos(ls, nd, desc):
        for lnd in ls :
            cdar_id, deno_ln, uds_ln, pcoste_ln = lnd
            id_d = data_base.dc_parser['id_art_desc']
            data_base.dc_parser['id_art_desc'] += 1
            dc_lnd = {
                'id': sanitize(str(id_d)),
                'empresa_id': '1',
                'articulo_padre_id': sanitize(nd),
                'articulo_id': sanitize(cdar_id),
                'nombre': sanitize(deno_ln),
                'unidades': sanitize(uds_ln),
                'precio_coste': sanitize(pcoste_ln),
            }
            desc.append(dc_lnd)

    def _stock(lst, ns, stk):
        table_rel = 'stock'
        for lnd in lst :
            calm, ubi, stock_a, stock_imputado, minimo, maximo, pedir_de = lnd
            alm = data_base.dc_db['almacenes'].get(calm)
            if alm is None:
                continue
            id_d = data_base.dc_parser['id_stock']
            data_base.dc_parser['id_stock'] += 1
            dc_lnd = {
                'id': sanitize(str(id_d)),
                'empresa_id': '1',
                'almacen_id': sanitize(alm),
                'articulo_id': sanitize(ns),
                'ubicacion': sanitize(ubi),
                'disponible': sanitize(stock_a),
                'reservado': sanitize(stock_imputado),
                'minimo': sanitize(minimo),
                'maximo': sanitize(maximo),
                'pedir_de': sanitize(pedir_de),
            }
            columns_ln = list()
            values_ln = list()
            kep = list(dc_lnd.keys())
            kep.sort()
            for kp in kep:
                vp = dc_lnd[kp]
                if vp is None:
                    continue
                columns_ln.append(kp)
                values_ln.append(vp)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))
            stk.append(sql_ln)

    def _tarifa(lsta, nt, tarls):
        table_rel = 'articulostarifas'
        for lnd in lsta :
            tar, margen_, precio_venta, comision_ = lnd
            tarifa = data_base.dc_db['tarifas'].get(tar)
            if tarifa is None:
                print(tar)
                continue
            id_d = data_base.dc_parser['id_stock']
            data_base.dc_parser['id_stock'] += 1
            dc_lnd = {
                'id': sanitize(str(id_d)),
                'empresa_id': '1',
                'tarifa_id': sanitize(tarifa),
                'articulo_id': sanitize(nt),
                'margen': sanitize(margen_),
                'precio_venta': sanitize(precio_venta),
                'comision': sanitize(comision_),
            }
            columns_ln = list()
            values_ln = list()
            kep = list(dc_lnd.keys())
            kep.sort()
            for kp in kep:
                vp = dc_lnd[kp]
                if vp is None:
                    continue
                columns_ln.append(kp)
                values_ln.append(vp)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))
            tarls.append(sql_ln)

    def _facturar(lsta, nt, lsfac):
        for lnd in lsta :
            taccion, artfac = lnd
            tipoaccion = data_base.dc_db['acciones_tipos'].get(taccion)
            if tipoaccion is None:
                continue
            id_d = data_base.dc_parser['id_articulosfacturar']
            data_base.dc_parser['id_articulosfacturar'] += 1
            dc_lnd = {
                'id': sanitize(str(id_d)),
                'empresa_id': '1',
                'tipo_accion_id': sanitize(tipoaccion),
                'articulo_id': sanitize(nt),
                'articulo_facturar_id': artfac,
            }
            lsfac.append(dc_lnd)

    table_db = 'articulos'
    table_erp = 'articulos'
    n_registro = data_base.dc_db['tiporegistros'][table_db]

    if reset:
        data_base.delete_all('articulosdescompuestos')
        data_base.delete_all('articulosproveedores')
        data_base.delete_all('articulosfacturar')
        data_base.delete_all('articulostarifas')
        data_base.delete_all('stock')
        data_base.delete_all(table_db)
        data_base.delete_where('traducciones', 'registro=%d' % n_registro)
        data_base.delete_where('checklistregistros', 'registro=%d' % n_registro)
        data_base.delete_where('caracteristicasregistros', 'registro=%d' % n_registro)
        data_base.delete_where('fechasregistros', 'registro=%d' % n_registro)
        data_base.delete_where('modeloscertificadoregistros', 'registro=%d' % n_registro)
        data_base.delete_where('checklistsecundarioregistros', 'registro=%d' % n_registro)

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    ruta_c = "D:/Server/Companies/jonathan/tesein/EJA/" + 'nseries_caracteristicas'
    rgs_caracteristicas = bkopen(ruta_c, huf=0)

    n=0
    proveedores = list()
    descompuestos = list()
    ls_traducciones = list()
    ls_fechas = list()
    caracteristicas = list()
    checklists = list()
    facturar_ls = list()
    tarifas_ls = list()
    mcertificados = list()
    stock = list()
    idxs = list(file_rg.keys())
    idxs.sort()

    log(table_erp, len(idxs))

    for idx in idxs:
        if not idx.strip():
            continue
        n+=1
        if n % 500 == 0.:
            log(idx, len(idxs), n)
        (nombre, ean, codigo_anterior, grupo, familia, sub_familia, nada, nada, tipo_articulo, tipo_iva,
         descatalogado, fecha_descatalogado, motivo_descatalogado, estocable, precio_ultima_compra, precio_medio_coste,
         precio_coste, margen, comision, precio_base_de_venta, descripcion_extendida, visible_en_cocosat, obligatorio_n_serie_en_ventas,
         formato_stock, periodicidad_ls, checklist_ls, tarifa_ls, articulo_desc_ls, arpop_ls, vendedor_comisiones_ls,
         stock_ls, ofertas_venta_ls, almacen, fecha_u_compra, fecha_u_venta, fecha_u_inventario, cuenta_contable_ventas,
         cuenta_contable_compras, fecha_ul_fabricacion, precio_u_fabricacion, nada, unidades_escandallo, lote_minimo_de_produccion,
         gestion_de_lotes, categoria, articulo_generico, gestion_de_descompuesto, gestion_de_numeros_de_serie, desc_ext_idioma_ls,
         cargar_u_checklst, dias_caducidad_lotes, visible_en_testastore, fecha_de_validez_precio_coste, nada, nada,
         desagrupar_checklists, nada, nada, excluir_de_las_impresiones, desagrupar_al_importar_en_descompuestos,
         nada, nada, nada, fases, horas_facturadas, nada, nada, tipo_fecha_ls, caracteristicas_ls, articulo_facturar_ls,
         obligatorio_numero_de_serie_en_compras, idioma_ls, certs_ls, nada, agrupar_unidades_a_pedir)  = file_rg[idx]
        if descripcion_extendida:
            descripcion_extendida = stripRtf(descripcion_extendida)

        formato_stock = formato_stock.upper()
        if formato_stock == 'U':
            formato_stock = 'UD'
        idx = sanitize(idx)
        data_base.dc_parser[table_erp][idx] = n

        categoria = None
        grupo = data_base.dc_db['grupos'].get(grupo)
        familia = data_base.dc_db['familias'].get(familia)
        sub_familia = data_base.dc_db['subfamilias'].get(sub_familia)
        tipo_articulo = data_base.dc_db['opcionestipos'].get(tipo_articulo+'|tipo_articulo')
        formato_stock = data_base.dc_db['opcionestipos'].get(formato_stock+'|formato_stock')
        almacen = data_base.dc_db['almacenes'].get(almacen)
        _proveedores(arpop_ls, n, proveedores)
        _descompuestos(articulo_desc_ls, n, descompuestos)

        _traducciones(data_base, idioma_ls, n, n_registro, ls_traducciones, data_base.dc_db)
        _fechas(data_base, tipo_fecha_ls, n, n_registro, ls_fechas, data_base.dc_db)
        _caracteristicas(data_base, caracteristicas_ls, n, n_registro, caracteristicas, rgs_caracteristicas, data_base.dc_db)
        _checks(data_base, checklist_ls, n, n_registro, checklists, data_base.dc_db)
        _stock(stock_ls, n, stock)
        _tarifa(tarifa_ls, n, tarifas_ls)
        _facturar(articulo_facturar_ls, n, facturar_ls)
        _checksecundario(data_base, certs_ls, n, n_registro, mcertificados, data_base.dc_db)
        if vendedor_comisiones_ls:
            print(ValueError('1 ' + table_erp+' - ' + str(vendedor_comisiones_ls)))
        if ofertas_venta_ls:
            print(ValueError('1 ' + table_erp+' - ' + str(ofertas_venta_ls)))
        if periodicidad_ls:
            print(ValueError('1 ' + table_erp+' - ' + str(periodicidad_ls)))

        dc = {
            'actu_precio_proveedor': sanitize(False),
            'desagrupar_pprecios': sanitize(agrupar_unidades_a_pedir=='S'),
            'almacen_defecto_id': sanitize(almacen),
            'cargar_ultimo_checklist': sanitize(cargar_u_checklst=='S'),
            'categoria_id': sanitize(categoria),
            'codigo_anterior': sanitize(codigo_anterior),
            'comision': sanitize(comision),
            'cuenta_contable_c': sanitize(cuenta_contable_compras),
            'cuenta_contable_v': sanitize(cuenta_contable_ventas),
            'desagrupar_checklist': sanitize(desagrupar_checklists=='S'),
            'bloqueado': sanitize(descatalogado=='S'),
            'desc_extendida': sanitize(descripcion_extendida),
            'dias_caducidad_lotes': sanitize(dias_caducidad_lotes),
            'ean': sanitize(ean),
            'estocable': sanitize(estocable=='S'),
            'excluir_impresiones': sanitize(excluir_de_las_impresiones=='S'),
            'familia_id': sanitize(familia),
            'fecha_validez_pcoste': sanitize(Num_aFecha(fecha_de_validez_precio_coste)),
            'fecha_bloqueo': sanitize(Num_aFecha(fecha_descatalogado)),
            'fecha_u_compra': sanitize(Num_aFecha(fecha_u_compra)),
            'fecha_u_inventario': sanitize(Num_aFecha(fecha_u_inventario)),
            'fecha_u_venta': sanitize(Num_aFecha(fecha_u_venta)),
            'formato_stock_id': sanitize(formato_stock),
            'ges_descompuesto': sanitize(gestion_de_descompuesto=='S'),
            'ges_lotes': sanitize(gestion_de_lotes=='S'),
            'ges_n_serie': sanitize(gestion_de_numeros_de_serie=='S'),
            'grupo_id': sanitize(grupo),
            'horas_facturadas': sanitize(horas_facturadas),
            'margen': sanitize(margen),
            'motivo_bloqueo': sanitize(motivo_descatalogado),
            'nombre': sanitize(nombre),
            'obligatorio_n_serie': sanitize(obligatorio_n_serie_en_ventas=='S'),
            'precio_base_venta': sanitize(precio_base_de_venta),
            'precio_coste': sanitize(precio_coste),
            'precio_medio_coste': sanitize(precio_medio_coste),
            'precio_ultima_compra': sanitize(precio_ultima_compra),
            'sub_familia_id': sanitize(sub_familia),
            'tipo_articulo_id': sanitize(tipo_articulo),
            'tipo_iva': sanitize(tipo_iva),
            'visible_app': sanitize(visible_en_cocosat=='S'),


        }
        columns = [u'id', u'codigo', u'empresa_id']
        values = [str(n), sanitize(idx), '1']
        ke = list(dc.keys())
        ke.sort()
        for k in ke:
            v = dc[k]
            if v is None:
                continue
            columns.append(k)
            values.append(v)

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    log('Direcciones y contactos ' + table_db, len(proveedores))
    i = 0
    for sql_r in proveedores:
        i += 1
        if i % 100 == 0.:
            log('', len(proveedores), i)
        data_base.query(sql_r)

    log('Traducciones ' + table_db, len(ls_traducciones))
    i = 0
    for sql_r in ls_traducciones:
        i += 1
        if i % 100 == 0.:
            log('', len(ls_traducciones), i)
        data_base.query(sql_r)

    log('Fechas ' + table_db, len(ls_fechas))
    i = 0
    for sql_r in ls_fechas:
        i += 1
        if i % 100 == 0.:
            log('', len(ls_fechas), i)
        data_base.query(sql_r)

    log('Tarifas ' + table_db, len(tarifas_ls))
    i = 0
    for sql_r in tarifas_ls:
        i += 1
        if i % 100 == 0.:
            log('', len(tarifas_ls), i)
        data_base.query(sql_r)

    log('Stock ' + table_db, len(stock))
    i = 0
    for sql_r in stock:
        i += 1
        if i % 100 == 0.:
            log('', len(stock), i)
        data_base.query(sql_r)

    log('Modelos certificados ' + table_db, len(mcertificados))
    i = 0
    for sql_r in mcertificados:
        i += 1
        if i % 100 == 0.:
            log('', len(mcertificados), i)
        data_base.query(sql_r)

    log('Artículos facturación ' + table_db, len(facturar_ls))
    i = 0
    for dc_ln in facturar_ls:
        i += 1
        if i % 100 == 0.:
            log('', len(facturar_ls), i)

        dc_ln['articulo_facturar_id'] = sanitize(data_base.dc_parser['articulos'].get(dc_ln['articulo_facturar_id']))
        if dc_ln['articulo_facturar_id'] is None:
            continue
        columns_ln = list()
        values_ln = list()
        kep = list(dc_ln.keys())
        kep.sort()
        for kp in kep:
            vp = dc_ln[kp]
            if vp is None:
                continue
            columns_ln.append(kp)
            values_ln.append(vp)

        sql_r = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % ('articulosfacturar', _join(columns_ln), _join(values_ln))
        data_base.query(sql_r)

    total=0
    table_db = 'articulosdescompuestos'
    log('Descompuestos', len(descompuestos))
    for dc_ln in descompuestos:
        cdar = data_base.dc_parser['articulos'].get(dc_ln['articulo_id'])
        total += 1
        if total % 100 == 0.:
            log(cdar, total, len(descompuestos))
        if cdar is None:
            continue
        dc_ln['articulo_id'] = sanitize(cdar)

        columns = list()
        values=list()
        ke = list(dc_ln.keys())
        ke.sort()
        for k in ke:
            v = dc_ln[k]
            if v is None:
                continue
            columns.append(k)
            values.append(v)

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_descompuestos():
    def _costes(ls, na, rels):
        table_rel = 'descompuestoscostesindirectos'
        for lnp in ls:
            tipo_coste, porcentaje = lnp
            tipo_coste = data_base.dc_parser['costes_descompuestos'].get(tipo_coste)
            if tipo_coste is None:
                continue


            dc_lnp = {
                'empresa_id': '1',
                'id': sanitize(na),
                'tipo_coste_id': sanitize(tipo_coste),
                'porcentaje': sanitize(porcentaje),
            }
            columns_ln = list()
            values_ln = list()
            kep = list(dc_lnp.keys())
            kep.sort()
            for kp in kep:
                vp = dc_lnp[kp]
                if vp is None:
                    continue
                columns_ln.append(kp)
                values_ln.append(vp)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))
            rels.append(sql_ln)

    table_db = 'descompuestos'
    table_erp = 'descompuestos'
    n_registro = data_base.dc_db['tiporegistros'][table_db]

    if reset:
        data_base.delete_all('descompuestos')
        data_base.delete_all('descompuestoscostesindirectos')
        #data_base.delete_where('checklistsecundarioregistros', 'registro=%d' % n_registro)

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)

    n=0

    lineas_ls = list()
    costes_ = list()
    idxs = list(file_rg.keys())
    idxs.sort()

    log(table_erp, len(idxs))

    for idx in idxs:
        if not idx.strip():
            continue

        (cdar, nombre, unidades_padre, actualiza_precio_de_venta, actualiza_precio_de_coste, codigo_documento_padre,
         archivo_documento_padre, descompuesto_original, lineas, costes_ls, observaciones)  = file_rg[idx]

        idx = sanitize(idx)

        id_registro = 1 # codigo_documento_padre TODO
        registro = 1#data_base.dc_db['tiporegistros'].get(archivo_documento_padre) TODO
        articulo = data_base.dc_parser['articulos'].get(cdar)
        if articulo is None:
            raise ValueError(cdar, idx)
            continue
        n+=1
        if n % 500 == 0.:
            log(idx, len(idxs), n)
        data_base.dc_parser[table_erp][idx] = n
        '''''¡: sanitize(lineas),
            '': sanitize(costes_ls),'''
        _costes(costes_ls, n, costes_)
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

        columns = []
        values = []
        ke = list(dc.keys())
        ke.sort()
        for k in ke:
            v = dc[k]
            if v is None:
                continue
            columns.append(k)
            values.append(v)

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    log('Líneas ' + table_db, len(lineas_ls))
    i = 0
    for sql_r in lineas_ls:
        i += 1
        if i % 100 == 0.:
            log('', len(lineas_ls), i)
        data_base.query(sql_r)

    log('Costes ' + table_db, len(costes_))
    i = 0
    for sql_r in costes_:
        i += 1
        if i % 100 == 0.:
            log('', len(costes_), i)
        data_base.query(sql_r)


if __name__ == '__main__':
    data_base = Database(2)
    empresa_id = '1'
    insert_proveedores()
    '''delegaciones_defecto = insert_clientes()
    insert_delegaciones(delegaciones_defecto)'''
    insert_articulos()
    insert_descompuestos()

    data_base.close()
