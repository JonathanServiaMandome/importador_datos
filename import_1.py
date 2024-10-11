# -*- coding: utf-8 -*-
from numpy.distutils.conv_template import named_re

from functions import *

from import_functions import _join, parser_date, sanitize, Database, log, _direcciones, _contactos


def insert_empresa():
    table_db = 'empresa'

    campos_db = ['id', 'nombre', 'identificador_fiscal', 'fecha_creacion', 'activo', 'direccion', 'telefono']
    values_db = ['1', "'SAT'", "'00000A'", "'01/10/2024'", "True", "'Calle'", "'981-999-999'"]


    log(table_db, 1)
    sql = u"INSERT INTO empresas_%s (%s) values (%s)" % (table_db, ', '.join(campos_db), ', '.join(values_db))
    data_base.query(sql)

def insert_paises():
    table_db = 'paises'
    table_erp = 'paises'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = list(file_rg.keys())
    idxs.sort()

    l = len(idxs)
    log(table_erp, l)
    data_base.dc_parser[table_erp] = dict()
    n_pais = 0
    for idx in idxs:
        n_pais += 1
        if n_pais % 100 == 0 or n_pais==l:
            log(idx, l, n_pais)

        data_base.dc_parser[table_erp][idx] = n_pais
        nombre, iso, sepa, lon_iban, div, idi =  file_rg[idx]

        columns = [u'id', u'codigo', u'nombre', u'codigo_iso', u'norma_sepa', u'longitud_iban', u'empresa_id']
        values = [sanitize(n_pais), sanitize(idx), sanitize(nombre), sanitize(iso), sanitize(sepa=='S'), sanitize(lon_iban), empresa_id]
        if div and div in data_base.dc_parser['divisas'].keys():
            columns.append(u'divisa_id')
            values.append(sanitize(data_base.dc_parser['divisas'][div]))
        if idi and idi in data_base.dc_parser['idiomas'].keys():
            columns.append(u'idioma_id')
            values.append(sanitize(data_base.dc_parser['idiomas'][idi]))

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_provincias():
    values_db = [
        u'Andalucía',
        u'Aragón',  # 2
        u'Islas Baleares',  # 3
        u'Canarias',  # 4
        u'Cantabría',  # 5
        u'Castilla - La Mancha',  # 6
        u'Castilla y León',  # 7
        u'Cataluña',  # 8
        u'Comunidad de Madrid',  # 9
        u'Comunidad Foral de Navarra',  # 10
        u'Comunidad Valenciana',  # 11
        u'Euskadi',  # 12
        u'Extremadura',  # 13
        u'Galicia',  # 14
        u'Principado de Asturias',  # 15
        u'Región de Murcia',  # 16
        u'La Rioja',  # 17
        u'Ceuta',  # 18
        u'Melilla',  # 19
    ]
    campos_db = ['id', 'nombre', 'empresa_id']
    list_values_db = list()
    table_db = 'regiones'
    data_base.dc_parser[table_db] = {}
    for x in range(len(values_db)):
        data_base.dc_parser[table_db][x+1] = values_db[x]
        list_values_db.append([sanitize(x+1), sanitize(values_db[x]), empresa_id])

    idxs = list(list_values_db)
    idxs.sort()
    l = len(idxs)
    log(table_db, l)

    n_reg = 0
    for values_db in list_values_db:
        n_reg += 1
        if n_reg % 100 == 0 or n_reg==l:
            log(sanitize(n_reg), l, n_reg)
        sql = u"INSERT INTO empresas_%s (%s) values (%s)" % (table_db, ', '.join(campos_db), ', '.join(values_db))
        data_base.query(sql)

    values_db = [
        ['01', u'Álava', u'VI', 12],
        ['02', u'Albacete', u'AB', 6],
        ['03', u'Alicante', u'A', 11],
        ['04', u'Almería', u'AL', 1],
        ['05', u'Ávila', u'AV', 7],
        ['06', u'Badajoz', u'BA', 13],
        ['07', u'Baleares', u'PM / IB', 3],
        ['08', u'Barcelona', u'B', 8],
        ['09', u'Burgos', u'BU', 7],
        ['10', u'Cáceres', u'CC', 13],
        ['11', u'Cádiz', u'CA', 1],
        ['12', u'Castellón', u'CS', 11],
        ['13', u'Ciudad Real', u'CR', 6],
        ['14', u'Córdoba', u'CO', 1],
        ['15', u'La Coruña', u'C', 14],
        ['16', u'Cuenca', u'CU', 6],
        ['17', u'Gerona', u'GE / GI', 8],
        ['18', u'Granada', u'GR', 1],
        ['19', u'Guadalajara', u'GU', 6],
        ['20', u'Guipúzcoa', u'SS', 12],
        ['21', u'Huelva', u'H', 1],
        ['22', u'Huesca', u'HU', 2],
        ['23', u'Jaén', u'J', 1],
        ['24', u'León', u'LE', 7],
        ['25', u'Lérida', u'L', 8],
        ['26', u'La Rioja', u'LO', 17],
        ['27', u'Lugo', u'LU', 14],
        ['28', u'Madrid', u'M', 9],
        ['29', u'Málaga', u'MA', 1],
        ['30', u'Murcia', u'MU', 16],
        ['31', u'Navarra', u'NA', 10],
        ['32', u'Orense', u'OR / OU', 14],
        ['33', u'Asturias', u'O', 15],
        ['34', u'Palencia', u'P', 7],
        ['35', u'Las Palmas', u'GC', 4],
        ['36', u'Pontevedra', u'PO', 14],
        ['37', u'Salamanca', u'SA', 7],
        ['38', u'Santa Cruz de Tenerife', u'TF', 4],
        ['39', u'Cantabria', u'S', 5],
        ['40', u'Segovia', u'SG', 7],
        ['41', u'Sevilla', u'SE', 1],
        ['42', u'Soria', u'SO', 7],
        ['43', u'Tarragona', u'T', 8],
        ['44', u'Teruel', u'TE', 2],
        ['45', u'Toledo', u'TO', 6],
        ['46', u'Valencia', u'V', 11],
        ['47', u'Valladolid', u'VA', 7],
        ['48', u'Vizcaya', u'BI', 12],
        ['49', u'Zamora', u'ZA', 7],
        ['50', u'Zaragoza', u'Z', 2],
        ['51', u'Ceuta', u'CE', 18],
        ['52', u'Melilla', u'ML', 19]
    ]
    campos_db = ['id', 'nombre', 'codigo_mi', 'pais_id','region_id', 'empresa_id']
    list_values_db = list()
    table_db = 'provincias'
    data_base.dc_parser[table_db] = {}
    idp = data_base.dc_parser['paises']['ESP']
    for x in range(len(values_db)):
        data_base.dc_parser[table_db][values_db[x][0]] = x+1
        list_values_db.append([sanitize(x+1), sanitize(values_db[x][1]), sanitize(values_db[x][2]), sanitize(idp), sanitize(values_db[x][3]), empresa_id])

    n_prov = 0
    idxs = list(list_values_db)
    idxs.sort()
    l = len(idxs)
    log(table_db, l)
    for values_db in list_values_db:
        n_prov += 1
        if n_prov % 100 == 0 or n_prov==l:
            log(sanitize(n_prov), l, n_prov)
        sql = u"INSERT INTO empresas_%s (%s) values (%s)" % (table_db, ', '.join(campos_db), ', '.join(values_db))
        data_base.query(sql)

def insert_tipos_registros():
    tipos_registros = [
        ['Recuentos', u'Recuentos de inventario'],
        ['Trasvases', u'Trasvases de mercancía'],
        ['PedidosAlmacen', u'Pedidos almacén'],
        ['PeticionPrecios', u'Peticién de precios'],
        ['PedidosProveedor', u'Pedidos a proveedor'],
        ['AlbaranCompra', u'Albarán de compra'],
        ['FacturasRecibidas', u'Facturas recibidas'],
        ['Pagos', u'Pagos a proveedores'],
        ['PagosAlbaranesCompra', u'Pagos de albaranes de compra'],
        ['RemesasPagos', u'Remesas de pagos'],
        ['Presupuestos', u'Presupuestos'],
        ['PedidosCliente', u'Pedidos a cliente'],
        ['AlbaranVenta', u'Albarán de venta'],
        ['FacturasEmitidas', u'Facturas emitidas'],
        ['Cobros', u'Cobros a clientes'],
        ['CobrosAlbaranesVenta', u'Cobros de albaranes de venta'],
        ['RemesasCobros', u'Remesas de cobros'],
        ['Contratos', u'Contratos'],
        ['Llamadas', u'Llamadas'],
        ['Arqueos', u'Arqueos'],
        ['Obras', u'Obras e instalaciones'],
        ['Partes', u'Partes de trabajo'],
        ['Acciones', u'Acciones'],
        ['Descompuestos', u'Descompuestos'],
        ['Elementos', u'Elementos revisables'],
        ['Vehiculos', u'Vehículos'],
        ['Delegaciones', u'Delegaciones de clientes'],
        ['Clientes', u'Clientes'],
        ['Proveedores', u'Proveedores'],
        ['Personal', u'Personal'],
        ['MetodosPago', u'Metodos de pago'],
        ['Fechas', u'Fechas recalculables'],
        ['CodigoPostal', u'Codigos postales'],
        ['PartesTipos', u'Tipos de partes'],
        ['AccionesTipos', u'Tipos de acciones'],
        ['Articulos', u'Artículos'],
        ['Almacenes', u'Almacenes'],
        ['Tipos', u'Tipos de registro'],
        ['Series', u'Series'],
        ['Provincias', u'Provincias'],
        ['Regiones', u'Regiones'],
        ['Paises', u'Paises'],
        ['Checklists', u'Checklists'],
        ['Bancos', u'Bancos'],
        ['Normativa', u'Normativas para certificados'],
        ['NormativaCapitulos', u'Capítulos de normativa para certificados'],
        ['Zonas', u'Zonas'],
        ['Transportistas', u'Transportistas'],
        ['Tarifas', u'Tarifas'],
        ['Swift', u'Swift'],
        ['Sociedades', u'Sociedades'],
        ['Rutas', u'Rutas'],
        ['Certificados', u'Certificados'],
        ['ModelosCertificados', u'Modelos de certificados'],
        ['MarcasRiesgo', u'Marcas de riesgo'],
        ['Marcas', u'Marcas'],
        ['Idiomas', u'Idiomas'],
        ['Emails', u'Emails'],
        ['Divisas', u'Divisas'],
        ['Departamentos', u'Departamentos'],
        ['ClasificacionClientes', u'Clasificacion de los clientes'],
        ['ClasificacionArticulos', u'Clasificacion de articulos'],
        ['CostesEmpleados', u'Costes empleados'],
        ['Capitulos', u'Capitulos'],
        ['Caracteristica', u'Caracteristicas de los elementos'],
        ['Administradores', u'Administradores de fincas'],
        ['ValoresPredefinidos', u'Valores predefinidos checklists'],
    ]
    campos_db = ['id','registro', 'nombre', 'empresa_id']
    list_values_db = list()
    data_base.dc_parser['tiporegistros'] = dict()
    for x in range(len(tipos_registros)):
        tipos_registros[x][0] = tipos_registros[x][0].lower()

        data_base.dc_parser['tiporegistros'][tipos_registros[x][0]] = x+1
        list_values_db.append([sanitize(x+1), sanitize(tipos_registros[x][0]) , sanitize(tipos_registros[x][1]), empresa_id])

    table_db = 'tiporegistros'

    idxs = list(list_values_db)
    idxs.sort()
    l = len(idxs)
    log(table_db, l)
    n_tipor = 0
    for values_db in list_values_db:
        n_tipor += 1
        if n_tipor % 100 == 0 or n_tipor==l:
            log(sanitize(n_tipor), l, n_tipor)
        sql = u"INSERT INTO empresas_%s (%s) values (%s)" % (table_db, ', '.join(campos_db), ', '.join(values_db))
        data_base.query(sql)

def insert_sub_tipos_registros():
    tipos_registros = [
        [u'actividad', u'Actividad empresarial'], #1 -
        [u'categoria_cliente', u'Categoría empresarial'], #2 -
        [u'sector', u'Sector empresarial'], #3 -
        [u'sub_sector', u'Sub sectores empresarial'], #4 -
        [u'categoria_articulo', u'Categoría artículo'], #5 -
        [u'grupo', u'Grupo artículo'], #6 -
        [u'familia', u'Familia artículo'], #7 -
        [u'sub_familia', u'Sub familia artículo'], #8 -
        [u'tipo_almacen', u'Tipos de almacén'], #9 -
        [u'tipo_contrato', u'Tipos de contratos'], #10 -
        [u'tipo_delegacion', u'Tipos de delegaciones'], #11 -
        [u'tipo_llamada', u'Tipos de llamadas'], #12 -
        [u'tipo_obra', u'Tipos de obra'], #13 -
        [u'tipo_vehiculo', u'Tipos de vehículos'], #14 -
        [u'tipo_gasto', u'Tipos de gastos'], #15 -
        [u'formato_articulo', u'Formatos de los artículos'], #16 -
        [u'contrato_ss', u'Tipos de contratos de la seguridad social'], #17 -
        [u'coste_descompuesto', u'Tipos de costes de los descompuestos'], #18 -
        [u'estado_parte', u'Estados de los partes'], #19 -
        [u'estado_cartera', u'Estados de la cartera'], #20 -
        [u'estado_obra', u'Estados de las obras'], #21 -
        [u'estado_trasvase', u'Estados de los trasvases'], #22 -
        [u'estado_pedido', u'Estados de los pedidos'], #23 -
        [u'estado_presupuesto', u'Estados de los Presupuestos'], #24 -
        [u'tipo_cartera', u'Tipos de los recibos de cartera'], #25 -
        [u'tipo_facturacion_auto', u'Tipos de facturación automática'], #26 -
        [u'tipo_albaran_venta', u'Tipos de los albaranes de venta'], #27 -
        [u'tipo_albaran_compra', u'Tipos de los albaranes de compra'], #28 -
        [u'tipo_compra_proveedor', u'Tipos de las compras del proveedor'], #29 -
        [u'tipo_proveedor', u'Tipos de los Proveedores'], #30 -
        [u'tipo_iva_cliente', u'Tipos de IVA del cliente '], #31 -
        [u'tipo_iva_proveedor', u'Tipos de IVA del proveedor '], #32 -
        [u'tipo_periodicidad', u'Tipos de periodicidades '], #33 -
        [u'tipo_articulo', u'Tipos de artículos'], #34 -
        [u'tipo_marcas_serie', u'Tipos de las marcas de serie'], #35 -
        [u'tipo_periodicidad_facturacion', u'Tipos de periodicidad de facturación'], #36 -
        [u'prioridad', u'Prioridades'], #37 -
        [u'formato_stock', u'Formato de los stocks'], #38 -
        [u'tipo_pregunta', u'Tipos de preguntas'], #39 -
    ]
    campos_db = ['id','registro', 'nombre', 'empresa_id']
    list_values_db = list()
    data_base.dc_parser['subtiporegistros'] = dict()
    for x in range(len(tipos_registros)):
        tipos_registros[x][0] = tipos_registros[x][0].lower()
        data_base.dc_parser['subtiporegistros'][tipos_registros[x][0]] = x+1
        list_values_db.append([sanitize(x+1), sanitize(tipos_registros[x][0]) , sanitize(tipos_registros[x][1]), empresa_id])

    table_db = 'subtiporegistros'

    idxs = list(list_values_db)
    idxs.sort()
    l = len(idxs)
    log(table_db, l)
    n_stipor = 0
    for values_db in list_values_db:
        n_stipor += 1
        if n_stipor % 100 == 0 or n_stipor==l:
            log(sanitize(values_db[0]), l, n_stipor)
        sql = u"INSERT INTO empresas_%s (%s) values (%s)" % (table_db, ', '.join(campos_db), ', '.join(values_db))
        data_base.query(sql)

def insert_divisas():
    values_db = [
        [u"INSERT INTO empresas_divisas (id, codigo, nombre, valor, codigo_bc3, codigo_anterior, empresa_id) VALUES (1, '001', 'EURO', 1.0, '', '', 1)"],
        [u"INSERT INTO empresas_divisas (id, codigo, nombre, valor, codigo_bc3, codigo_anterior, empresa_id) VALUES (2, '002', 'DOLAR', 1.0, '', '', 1)"],
    ]
    table_db = 'divisas'

    log(table_db, len(values_db))
    data_base.dc_parser[table_db] = {
        '001': 1,
        '002': 2,
    }

    for ln_sql in values_db:
        sql = ln_sql[0]
        data_base.query(sql)

def insert_idiomas():
    values_db = [
        [u"INSERT INTO empresas_idiomas (id, codigo, nombre, codigo_anterior, empresa_id) VALUES (1, '001', 'Español', '', 1)"],
        [u"INSERT INTO empresas_idiomas (id, codigo, nombre, codigo_anterior, empresa_id) VALUES (2, '002', 'Portugués', '', 1)"],
        [u"INSERT INTO empresas_idiomas (id, codigo, nombre, codigo_anterior, empresa_id) VALUES (3, '003', 'Inglés', '', 1)"]
    ]
    table_db = 'idiomas'
    data_base.dc_parser[table_db] = {
        '001': 1,
        '002': 2,
        '003': 3
    }

    log(table_db, len(values_db))
    for ln_sql in values_db:
        sql = ln_sql[0]
        data_base.query(sql)

def insert_clasificacionclientes():
    table = 'clasificacionclientes'
    table_erp_ss = 'subsectores'
    ruta_rg_ss = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_ss
    file_rg_ss = bkopen(ruta_rg_ss, huf=0)
    idxs_ss = file_rg_ss.keys()
    idxs_ss.sort()
    data_base.dc_parser[table_erp_ss] = dict()

    table_erp_cc = 'categorias_clientes'
    ruta_rg_cc = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_cc
    file_rg_cc = bkopen(ruta_rg_cc, huf=0)
    idxs_cc = file_rg_cc.keys()
    idxs_cc.sort()
    data_base.dc_parser[table_erp_cc] = dict()

    table_erp_se = 'sectores'
    ruta_rg_se = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_se
    file_rg_se = bkopen(ruta_rg_se, huf=0)
    idxs_se = file_rg_se.keys()
    idxs_se.sort()
    data_base.dc_parser[table_erp_se] = dict()

    table_erp_act = 'actividades'
    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_act
    file_rg_act = bkopen(ruta_rg, huf=0)
    idxs_act = list(file_rg_act.keys())
    idxs_act.sort()
    data_base.dc_parser[table_erp_act] = dict()


    l = len(idxs_act) + len(idxs_ss) + len(idxs_se) + len(idxs_cc)
    log(table, l)

    n_clasc=0
    # Actividades
    n_registro = data_base.dc_parser['tiporegistros']['clasificacionclientes']
    nr = data_base.dc_parser['subtiporegistros']['actividad']
    for idx in idxs_act:
        n_clasc+=1
        if n_clasc % 100 == 0 or n_clasc==l:
            log(sanitize(n_clasc), l, n_clasc)
        data_base.dc_parser[table_erp_act][idx] = n_clasc

        nombre, cdant, precios, email =  file_rg_act[idx]
        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_clasc), sanitize(idx), sanitize(nombre), sanitize(nr), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table, _join(columns), _join(values))
        data_base.query(sql)

    # Cat. clientes
    nr = data_base.dc_parser['subtiporegistros']['categoria_cliente']
    for idx in file_rg_cc.keys():
        n_clasc+=1
        if n_clasc % 100 == 0 or n_clasc==l:
            log(sanitize(n_clasc), l, n_clasc)
        data_base.dc_parser[table_erp_cc][idx] = n_clasc

        nombre =  file_rg_cc[idx][0]
        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_clasc), sanitize(idx), sanitize(nombre), sanitize(nr), sanitize(''), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table, _join(columns), _join(values))
        data_base.query(sql)

    # Sectores
    nr = data_base.dc_parser['subtiporegistros']['sector']
    for idx in file_rg_se.keys():
        n_clasc+=1
        if n_clasc % 100 == 0 or n_clasc==l:
            log(sanitize(n_clasc), l, n_clasc)
        data_base.dc_parser[table_erp_se][idx] = n_clasc

        nombre, cdant =  file_rg_se[idx][:2]
        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_clasc), sanitize(idx), sanitize(nombre), sanitize(nr), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table, _join(columns), _join(values))
        data_base.query(sql)

    # Sub sectores
    relaciones = list()
    nr = data_base.dc_parser['subtiporegistros']['sub_sector']
    for idx in idxs_ss:
        n_clasc+=1
        if n_clasc % 100 == 0 or n_clasc==l:
            log(sanitize(n_clasc), l, n_clasc)
        data_base.dc_parser[table_erp_ss][idx] = n_clasc

        nombre, cdant, padre =  file_rg_ss[idx][:3]
        id_padre = data_base.dc_parser['sectores'][padre]
        relaciones.append([id_padre, n_registro, n_clasc, n_registro])
        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_clasc), sanitize(idx), sanitize(nombre), sanitize(nr), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table, _join(columns), _join(values))
        data_base.query(sql)

    for value in relaciones:
        padre, r_padre, hijo, r_hijo = value
        idr = data_base.dc_parser['id_relaciones']
        columns = [u'id', u'id_padre', u'registro_padre', u'id_hijo', u'registro_hijo', u'empresa_id']
        values = [sanitize(idr),sanitize(padre), sanitize(r_padre), sanitize(hijo), sanitize(r_hijo), empresa_id]
        data_base.dc_parser['id_relaciones'] += 1
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % ('relacionregistros', _join(columns), _join(values))
        data_base.query(sql)

def insert_caracteristica():
    table_db = 'caracteristica'
    table_erp = 'nseries_caracteristica'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)
    n_car = 0
    data_base.dc_parser[table_erp] = dict()
    for idx in idxs:
        n_car += 1
        if n_car % 100 == 0 or n_car==l:
            log(sanitize(idx), l, n_car)
        data_base.dc_parser[table_erp][idx] = n_car

        nombre, tipo = file_rg[idx][:2]

        columns = [u'id', u'codigo', u'nombre', u'tipo_valor', 'empresa_id']
        values = [sanitize(n_car), sanitize(idx), sanitize(nombre), sanitize(tipo), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_opciones_tipos():
    data = [
        [u'X', u'Rechazado', 'estado_parte'],
        [u'N', u'No asignado', 'estado_parte'],
        [u'M', u'Modificado', 'estado_parte'],
        [u'D', u'Descargado', 'estado_parte'],
        [u'P', u'Pendiente', 'estado_parte'],
        [u'F', u'Finalizado', 'estado_parte'],
        [u'R', u'Resuelto', 'estado_parte'],
        [u'I', u'Inválido', 'estado_parte'],
        [u'N', u'Pendiente', 'estado_cartera'],
        [u'R', u'Remesado', 'estado_cartera'],
        [u'P', u'Pagado', 'estado_cartera'],
        [u'C', u'Cobrado', 'estado_cartera'],
        [u'D', u'Devuelto', 'estado_cartera'],
        [u'X', u'Cancelado', 'estado_cartera'],
        [u'P', u'Pendiente', 'estado_obra'],
        [u'A', u'Activa', 'estado_obra'],
        [u'F', u'Finalizada', 'estado_obra'],
        [u'R', u'Rechazada', 'estado_obra'],
        [u'PE', u'Pendiente de enviar', 'estado_trasvase'],
        [u'ME', u'Mercancía enviada', 'estado_trasvase'],
        [u'MR', u'Mercancía recibida', 'estado_trasvase'],
        [u'P', u'Pendiente', 'estado_pedido'],
        [u'S', u'Servido', 'estado_pedido'],
        [u'F', u'Finalizado', 'estado_pedido'],
        [u'A', u'Anulado', 'estado_pedido'],
        [u'I', u'Inactivo', 'estado_pedido'],
        [u'PT', u'Pendiente', 'estado_presupuesto'],
        [u'ES', u'Escrito', 'estado_presupuesto'],
        [u'SO', u'Solicitando precios', 'estado_presupuesto'],
        [u'AC', u'Aceptado', 'estado_presupuesto'],
        [u'RE', u'Revisado', 'estado_presupuesto'],
        [u'RZ', u'Rechazado', 'estado_presupuesto'],
        [u'EN', u'Entregado', 'estado_presupuesto'],
        [u'EM', u'Emitido', 'estado_presupuesto'],
        [u'CA', u'Caducado', 'estado_presupuesto'],
        [u'C', u'Contado', 'tipo_cartera'],
        [u'J', u'Tarjeta crédito', 'tipo_cartera'],
        [u'T', u'Talón', 'tipo_cartera'],
        [u'E', u'Efecto', 'tipo_cartera'],
        [u'L', u'Letra cambio', 'tipo_cartera'],
        [u'P', u'Pagaré', 'tipo_cartera'],
        [u'R', u'Recibo', 'tipo_cartera'],
        [u'X', u'Transferencia', 'tipo_cartera'],
        [u'G', u'Giro', 'tipo_cartera'],
        [u'O', u'Otras', 'tipo_cartera'],
        [u'A', u'Anticipo', 'tipo_cartera'],
        [u'N', u'Confirming', 'tipo_cartera'],
        [u'A', u'Por albarán', 'tipo_facturacion_auto'],
        [u'D', u'Por delegación', 'tipo_facturacion_auto'],
        [u'P', u'Por número de pedido', 'tipo_facturacion_auto'],
        [u'T', u'Total', 'tipo_facturacion_auto'],
        [u'N', u'No facturar', 'tipo_facturacion_auto'],
        [u'V', u'Venta', 'tipo_albaran_venta'],
        [u'A', u'Abono', 'tipo_albaran_venta'],
        [u'N', u'No facturable', 'tipo_albaran_venta'],
        [u'C', u'Compra', 'tipo_albaran_compra'],
        [u'A', u'Abono', 'tipo_albaran_compra'],
        [u'N', u'No facturable', 'tipo_albaran_compra'],
        [u'N', u'Nacional', 'tipo_compra_proveedor'],
        [u'I', u'Internacional', 'tipo_compra_proveedor'],
        [u'E', u'Extracomunitario', 'tipo_compra_proveedor'],
        [u'A', u'Aduana', 'tipo_compra_proveedor'],
        [u'A', u'Acreedor', 'tipo_proveedor'],
        [u'S', u'Sumunistros', 'tipo_proveedor'],
        [u'P', u'Proveedor', 'tipo_proveedor'],
        [u'O', u'Otros', 'tipo_proveedor'],
        [u'0', u'Regimen normal', 'tipo_iva_cliente'],
        [u'1', u'Exento de impuestos', 'tipo_iva_cliente'],
        [u'2', u'IGIC', 'tipo_iva_cliente'],
        [u'3', u'Recarga de equivalencia', 'tipo_iva_cliente'],
        [u'0', u'Regimen normal', 'tipo_iva_proveedor'],
        [u'1', u'Exento de impuestos', 'tipo_iva_proveedor'],
        [u'2', u'IGIC', 'tipo_iva_proveedor'],
        [u'3', u'Recarga de equivalencia', 'tipo_iva_proveedor'],
        [u'A', u'Años', 'tipo_periodicidad'],
        [u'M', u'Meses', 'tipo_periodicidad'],
        [u'D', u'Días', 'tipo_periodicidad'],
        [u'IN', u'Interno', 'tipo_almacen'],
        [u'EX', u'Externo', 'tipo_almacen'],
        [u'DE', u'Deposito', 'tipo_almacen'],
        [u'BO', u'Botella', 'tipo_almacen'],
        [u'VE', u'Vehículo', 'tipo_almacen'],
        [u'TR', u'Tránsito', 'tipo_almacen'],
        [u'OB', u'Obra', 'tipo_almacen'],
        [u'AT', u'Atípico', 'tipo_articulo'],
        [u'VA', u'Varios', 'tipo_articulo'],
        [u'EL', u'Elemento', 'tipo_articulo'],
        [u'CO', u'Consumible', 'tipo_articulo'],
        [u'CP', u'Componente', 'tipo_articulo'],
        [u'MA', u'Material', 'tipo_articulo'],
        [u'SE', u'Servicio', 'tipo_articulo'],
        [u'MO', u'Mano de obra', 'tipo_articulo'],
        [u'CC', u'Checklist contrato', 'tipo_articulo'],
        [u'CC', u'Criterio de caja', 'tipo_marcas_serie'],
        [u'AI', u'Adquisición intracomunitaria', 'tipo_marcas_serie'],
        [u'IM', u'Importación', 'tipo_marcas_serie'],
        [u'IP', u'Inversión sujeto pasivo', 'tipo_marcas_serie'],
        [u'D', u'Diaria', 'tipo_periodicidad_facturacion'],
        [u'S', u'Semanal', 'tipo_periodicidad_facturacion'],
        [u'Q', u'Quincenal', 'tipo_periodicidad_facturacion'],
        [u'M', u'Mensual', 'tipo_periodicidad_facturacion'],
        [u'01', u'Alta', 'prioridad'],
        [u'02', u'Media', 'prioridad'],
        [u'03', u'Baja', 'prioridad'],
        [u'U', u'UD', 'formato_stock'],
        [u'K', u'KG', 'formato_stock'],
        [u'M', u'MT', 'formato_stock'],
        [u'L', u'LT', 'formato_stock'],
        [u'T', u'Texto libre', 'tipo_pregunta'],
        [u'B', u'Sí / No', 'tipo_pregunta'],
        [u'N', u'Numérico', 'tipo_pregunta'],
        [u'V', u'Valores predefinidos', 'tipo_pregunta'],
        [u'X', u'Si - No - N/A', 'tipo_pregunta'],
        [u'C', u'Cabecera booleana', 'tipo_pregunta'],
        [u'H', u'Cabecera no boleana', 'tipo_pregunta'],
    ]

    table_db = 'opcionestipos'

    l = len(data)

    log(table_db, l)

    data_base.dc_parser[table_db] = dict()
    n_opct=0
    for ln in data:
        n_opct+=1
        if n_opct % 100 == 0 or n_opct==l:
            log(sanitize(ln[0]), l, n_opct)
        codigo, nombre, id_ = ln

        sub_tipo = data_base.dc_parser['subtiporegistros'][id_]
        data_base.dc_parser[table_db][codigo + '|' + id_] = n_opct

        columns = [u'id', u'tipo_id', u'nombre', 'empresa_id']
        values = [sanitize(n_opct), sanitize(sub_tipo), sanitize(nombre), empresa_id]

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_fechas():
    table_db = 'fechas'
    table_erp = 'nseries_fechas'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)
    log(table_db, l)
    data_base.dc_parser[table_erp] = dict()

    n_fec=0
    for idx in idxs:
        n_fec+=1
        if n_fec % 100 == 0 or n_fec==l:
            log(sanitize(idx), l, n_fec)
        data_base.dc_parser[table_erp][idx] = n_fec

        nombre, per, tper, na =  file_rg[idx]

        columns = [u'id', u'codigo', u'nombre', u'periodicidad', u'empresa_id']
        values = [sanitize(n_fec), sanitize(idx), sanitize(nombre), sanitize(per), empresa_id]
        if tper:
            itper = data_base.dc_parser['opcionestipos'][tper+'|tipo_periodicidad']
            columns.append(u'tipo_periodicidad_id')
            values.append(sanitize(itper))
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_administradores():
    table_db = 'administradores'
    table_erp = 'administradores'

    n_registro = data_base.dc_parser['tiporegistros'][table_erp]

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)
    log(table_db, l)
    data_base.dc_parser[table_erp] = dict()

    relacionados = list()

    n_admin=0
    for idx in idxs:
        n_admin+=1
        if n_admin % 100 == 0 or n_admin==l:
            log(sanitize(idx), l, n_admin)
        data_base.dc_parser[table_erp][idx] = n_admin

        nombre, direccion, poblacion, provincia, codigopostal, telefono, fax, dni, email, fechanacimiento, observaciones=  file_rg[idx]
        if poblacion or codigopostal or direccion:
            _direcciones(data_base, [[direccion, codigopostal, poblacion, provincia, 'ESP']], n_admin, n_registro, relacionados, True, True)

        if telefono or fax or dni or email:
            _contactos(data_base, [['', '', telefono, email, True, dni, fax]], n_admin, n_registro, relacionados)

        observaciones = stripRtf(observaciones)

        columns = [u'id', u'codigo', u'nombre', u'observaciones', u'empresa_id']#todo
        values = [sanitize(n_admin), sanitize(idx), sanitize(observaciones), empresa_id]

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    log('Direcciones y contactos ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)

def insert_normativa():
    def _traducciones(ls_idi, index):
        for ln_idi in ls_idi:
            cd_idi, nom = ln_idi
            id_idi = data_base.dc_parser['idiomas'][cd_idi]

            id_ = data_base.dc_parser['id_traducciones']
            data_base.dc_parser['id_traducciones'] += 1
            table_rel = 'traducciones'
            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'idioma_id': sanitize(id_idi),
                'nombre': sanitize(nom),
                'id_registro': sanitize(index),
                'registro': sanitize(nr_clas),
            }
            columns_ln = list()
            values_ln = list()
            ke = list(dc_ln.keys())
            ke.sort()
            for k in ke:
                v = dc_ln[k]
                if v is None:
                    continue
                columns_ln.append(k)
                values_ln.append(v)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))

            relacionados.append(sql_ln)

    table_db = 'normativacapitulos'
    table_erp = 'normativa_capitulos'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    data_base.dc_parser[table_erp] = dict()
    l = len(idxs)

    relacionados = list()

    log(table_db, l)
    nr_clas = data_base.dc_parser['tiporegistros']['normativacapitulos']

    n_nc=0
    for idx in idxs:
        n_nc+=1
        if n_nc % 100 == 0 or n_nc==l:
            log(sanitize(idx), l, n_nc)
        data_base.dc_parser[table_erp][idx] = n_nc

        nombre, idiomas =  file_rg[idx]
        _traducciones(idiomas, n_nc)

        columns = [u'id', u'codigo', u'nombre', u'empresa_id']
        values = [sanitize(n_nc), sanitize(idx), sanitize(nombre), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    table_db = 'normativa'
    table_erp = 'normativa'
    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    data_base.dc_parser[table_erp] = dict()
    l = len(idxs)

    log(table_db, l)

    n_nor=0
    for idx in idxs:
        n_nor+=1
        if n_nor % 100 == 0 or n_nor==l:
            log(sanitize(idx), l, n_nor)
        data_base.dc_parser[table_erp][idx] = n_nor

        nombre, imp, cert, apartado, subapartado, cap =  file_rg[idx]
        capid = data_base.dc_parser['normativa_capitulos'][cap]
        if cert:
            data_base.dc_parser['relaciones'] = data_base.dc_parser.get('relaciones',{})
            data_base.dc_parser['relaciones']['certificados'] = data_base.dc_parser['relaciones'].get('certificados',[])
            data_base.dc_parser['relaciones']['certificados'].append([n_nor, 45, repr(cert)])
        columns = [u'id', u'codigo', u'nombre', u'apartado', u'sub_apartado', u'capitulo_normativa_id', u'empresa_id']
        values = [sanitize(n_nor), sanitize(idx), sanitize(nombre), sanitize(apartado), sanitize(subapartado), sanitize(capid), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    for sql in relacionados:
        data_base.query(sql)

def insert_checklists():
    valores_predefinidos = list()

    def _lineas(lines, ck):
        table_rel = 'checklistslineas'
        j = 0
        for line in lines:
            j += 1
            num, padre_, descripcion, tipo_c, valorespredefinidos, obligatorio, valorpordefecto, periodicidad_c = line
            tipo_c = data_base.dc_parser['opcionestipos'][tipo_c+'|tipo_pregunta']
            if periodicidad_c:
                periodicidad_c = data_base.dc_parser['opcionestipos'][periodicidad_c+'|tipo_periodicidad']
            else:
                periodicidad_c=None
            id_ = data_base.dc_parser['id_checklists_lineas']
            data_base.dc_parser['id_checklists_lineas'] += 1
            if valorespredefinidos:
                valorespredefinidos = valorespredefinidos.split('|')
                for v in range(len(valorespredefinidos)):
                    id_v = data_base.dc_parser['id_valor_predefinido']
                    data_base.dc_parser['id_valor_predefinido'] += 1
                    valores_predefinidos.append([id_v, id_, valorespredefinidos[v], v+1])

            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'checklists_id': sanitize(ck),
                'tipo_id': sanitize(tipo_c),
                'numero': sanitize(float(num)),
                'descripcion': sanitize(descripcion),
                'obligatorio': sanitize(obligatorio=='S'),
                'valor_defecto': sanitize(valorpordefecto),
                'tipo_periodicidad_id': sanitize(periodicidad_c),
                'orden': sanitize(j),
            }
            columns_ln = list()
            values_ln = list()
            ke = list(dc_ln.keys())
            ke.sort()
            for k in ke:
                v = dc_ln[k]
                if v is None:
                    continue
                columns_ln.append(k)
                values_ln.append(v)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))

            lineas.append(sql_ln)
    table_db = 'checklists'

    table_erp = 'checklists'
    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)
    lineas=[]
    n_chl=0
    for idx in file_rg.keys():
        n_chl+=1
        if n_chl % 100 == 0 or n_chl==l:
            log(sanitize(idx), l, n_chl)
        data_base.dc_parser[table_erp][idx] = n_chl
        nombre, lnas, norm, eti, tec_ls =  file_rg[idx]
        _lineas(lnas, n_chl)
        columns = [u'id', u'codigo', u'nombre', u'empresa_id']
        values = [sanitize(n_chl), sanitize(idx), sanitize(nombre), empresa_id]
        if norm and norm in data_base.dc_parser['normativa'].keys():
            norm = data_base.dc_parser['normativa'][norm]
            columns.append(u'normativa_id')
            values.append(sanitize(norm))
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    l=len(lineas)
    log('Líneas '+table_db, l)
    i =  0
    for sql in lineas:
        i += 1
        if i % 100 == 0 or i==l:
            log('', l, i)
        data_base.query(sql)


    for value in valores_predefinidos:
        idh, padre, valor, orden = value
        columns = [u'id', u'linea_checklist_id', u'valor', u'orden', u'empresa_id']
        values = [sanitize(idh),sanitize(padre), sanitize(valor), sanitize(orden), empresa_id]
        data_base.dc_parser['id_relaciones'] += 1
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % ('valorespredefinidos', _join(columns), _join(values))
        data_base.query(sql)

def insert_tipos_acciones():
    table_db = 'accionestipos'
    table_erp = 'acciones_tipos'

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)
    data_base.dc_parser[table_erp] = dict()
    n_ta=0
    for idx in file_rg.keys():
        n_ta+=1
        if n_ta % 100 == 0 or n_ta==l:
            log(sanitize(idx), l, n_ta)
        data_base.dc_parser[table_erp][idx] = n_ta
        nombre,fecha, pri = file_rg[idx][:3]

        columns = [u'id', u'codigo', u'nombre', u'prioridad', u'empresa_id']
        values = [sanitize(n_ta), sanitize(idx), sanitize(nombre), sanitize(pri), empresa_id]
        if fecha and fecha in data_base.dc_parser['nseries_fechas'].keys():
            tipo_fecha_id = data_base.dc_parser['nseries_fechas'][fecha]
            columns.append(u'tipo_fecha_id')
            values.append(sanitize(tipo_fecha_id))
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_clasificacionarticulos():
    def _fechas(ls_fechas, index):
        for ln_fec in ls_fechas:
            cd_fec, periodicidad_f, tipo_periodicidad_f = ln_fec

            id_fec = data_base.dc_parser['nseries_fechas'][cd_fec]
            id_ = data_base.dc_parser['id_fechas_registros']
            data_base.dc_parser['id_fechas_registros'] += 1
            table_rel = 'fechasregistros'
            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'fecha_id': sanitize(id_fec),
                'periodicidad': sanitize(periodicidad_f),
                'tipo_periodicidad': sanitize(tipo_periodicidad_f),
                'id_registro': sanitize(index),
                'registro': sanitize(n_registro),
            }
            columns_ln = list()
            values_ln = list()
            ke = list(dc_ln.keys())
            ke.sort()
            for k in ke:
                v = dc_ln[k]
                if v is None:
                    continue
                columns_ln.append(k)
                values_ln.append(v)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))

            relacionados.append(sql_ln)

    def _caracteristicas(ls_car, index):
        for cd_car in ls_car:
            if rgs_caracteristicas.get(cd_car) is None :
                raise ValueError(cd_car, rgs_caracteristicas.keys())

            rg_car = rgs_caracteristicas[cd_car]
            tipo_valor = rg_car[1][0].upper()
            id_car = data_base.dc_parser['nseries_caracteristica'][cd_car]
            id_ = data_base.dc_parser['id_caracteristicas_registros']
            data_base.dc_parser['id_caracteristicas_registros'] += 1

            table_rel = 'caracteristicasregistros'
            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'caracteristica_id': sanitize(id_car),
                'tipo_valor': sanitize(tipo_valor),
                'id_registro': sanitize(index),
                'registro': sanitize(n_registro),
            }
            columns_ln = list()
            values_ln = list()
            ke = list(dc_ln.keys())
            ke.sort()
            for k in ke:
                v = dc_ln[k]
                if v is None:
                    continue
                columns_ln.append(k)
                values_ln.append(v)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))

            relacionados.append(sql_ln)

    def _traducciones(ls_tra, index):
        for ln_tra in ls_tra:
            cd_tra, nom = ln_tra
            id_tra = data_base.dc_parser['idiomas'][cd_tra]

            id_ = data_base.dc_parser['id_traducciones']
            data_base.dc_parser['id_traducciones'] += 1
            table_rel = 'traducciones'
            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'idioma_id': sanitize(id_tra),
                'nombre': sanitize(nom),
                'id_registro': sanitize(index),
                'registro': sanitize(n_registro),
            }
            columns_ln = list()
            values_ln = list()
            ke = list(dc_ln.keys())
            ke.sort()
            for k in ke:
                v = dc_ln[k]
                if v is None:
                    continue
                columns_ln.append(k)
                values_ln.append(v)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))

            relacionados.append(sql_ln)

    def _checks(ls_check, index):
        for ln_check in ls_check:
            cd_check, tp_accion, idio = ln_check
            id_check = data_base.dc_parser['checklists'][cd_check]

            id_ = data_base.dc_parser['id_checklist_registros']
            id_idi = data_base.dc_parser['idiomas'].get(idio)
            tipo_accion_id = data_base.dc_parser['acciones_tipos'].get(tp_accion)
            data_base.dc_parser['id_checklist_registros'] += 1
            table_rel = 'checklistregistros'
            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'checklist_id': sanitize(id_check),
                'tipo_accion_id': sanitize(tipo_accion_id),
                'idioma_id': sanitize(id_idi),
                'id_registro': sanitize(index),
                'registro': sanitize(n_registro),
            }
            columns_ln = list()
            values_ln = list()
            ke = list(dc_ln.keys())
            ke.sort()
            for k in ke:
                v = dc_ln[k]
                if v is None:
                    continue
                columns_ln.append(k)
                values_ln.append(v)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))

            relacionados.append(sql_ln)

    table = 'clasificacionarticulos'

    table_erp_cat = 'categorias'
    ruta_rg_cat = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_cat
    file_rg_cat = bkopen(ruta_rg_cat, huf=0)
    data_base.dc_parser[table_erp_cat] = dict()
    keys_cat = file_rg_cat.keys()
    keys_cat.sort()

    table_erp_gr = 'grupos'
    ruta_rg_gr = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_gr
    file_rg_gr = bkopen(ruta_rg_gr, huf=0)
    data_base.dc_parser[table_erp_gr] = dict()
    keys_gr = file_rg_gr.keys()
    keys_gr.sort()

    table_erp_fa = 'familias'
    ruta_rg_fa = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_fa
    file_rg_fa = bkopen(ruta_rg_fa, huf=0)
    data_base.dc_parser[table_erp_fa] = dict()
    keys_fa = file_rg_fa.keys()
    keys_fa.sort()

    table_erp_sf = 'subfamilias'
    ruta_rg_sf = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_sf
    file_rg_sf = bkopen(ruta_rg_sf, huf=0)
    data_base.dc_parser[table_erp_sf] = dict()
    keys_sf = file_rg_sf.keys()
    keys_sf.sort()

    ruta_rg_rgc = "D:/Server/Companies/jonathan/tesein/EJA/" + 'nseries_caracteristica'
    rgs_caracteristicas = bkopen(ruta_rg_rgc, huf=0)

    l = len(keys_cat) + len(keys_gr) + len(keys_fa) + len(keys_sf)
    log(table, l)

    relacionados = list()
    n_registro = data_base.dc_parser['tiporegistros']['clasificacionarticulos']

    n_clasa=0
    nr = data_base.dc_parser['subtiporegistros']['categoria_articulo']
    for idx in keys_cat:
        n_clasa+=1
        if n_clasa % 100 == 0 or n_clasa==l:
            log(sanitize(n_clasa), l, n_clasa)
        data_base.dc_parser[table_erp_cat][idx] = n_clasa
        nombre =  file_rg_cat[idx][0]
        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_clasa), sanitize(idx), sanitize(nombre), sanitize(sanitize(nr)), sanitize(''), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table, _join(columns), _join(values))
        data_base.query(sql)

    nr = data_base.dc_parser['subtiporegistros']['grupo']
    for idx in keys_gr:
        n_clasa+=1
        if n_clasa % 100 == 0 or n_clasa==l:
            log(sanitize(n_clasa), l, n_clasa)
        data_base.dc_parser[table_erp_gr][idx] = n_clasa

        nombre, car, une, fechas, checks, cdant, idiomas = file_rg_gr[idx]

        _caracteristicas(car, n_clasa)
        _fechas(fechas, n_clasa)
        _traducciones(idiomas, n_clasa)
        _checks(checks, n_clasa)


        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_clasa), sanitize(idx), sanitize(nombre), sanitize(sanitize(nr)), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table, _join(columns), _join(values))
        data_base.query(sql)

    relaciones = list()
    nr = data_base.dc_parser['subtiporegistros']['familia']
    for idx in keys_fa:
        n_clasa+=1
        if n_clasa % 100 == 0 or n_clasa==l:
            log(sanitize(n_clasa), l, n_clasa)
        data_base.dc_parser[table_erp_fa][idx] = n_clasa

        nombre, padre, trsp, ctas, libre, car, fechas, checks, cdant, idiomas =  file_rg_fa[idx]
        if padre not in data_base.dc_parser['grupos'].keys():
            continue

        id_padre = data_base.dc_parser['grupos'][padre]
        relaciones.append([id_padre, n_registro, n_clasa, n_registro])

        _caracteristicas(car, n_clasa)
        _fechas(fechas, n_clasa)
        _traducciones(idiomas, n_clasa)
        _checks(checks, n_clasa)

        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_clasa), sanitize(idx), sanitize(nombre), sanitize(nr), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table, _join(columns), _join(values))
        data_base.query(sql)

    nr = data_base.dc_parser['subtiporegistros']['sub_familia']
    for idx in keys_sf:
        n_clasa+=1
        if n_clasa % 100 == 0 or n_clasa==l:
            log(sanitize(n_clasa), l, n_clasa)
        data_base.dc_parser[table_erp_sf][idx] = n_clasa

        nombre, padre, une, car, fechas, checks, idiomas, cdant =  file_rg_sf[idx]

        if padre not in data_base.dc_parser['familias'].keys():
            continue
        id_padre = data_base.dc_parser['familias'][padre]
        relaciones.append([id_padre, n_registro, n_clasa, n_registro])

        _caracteristicas(car, n_clasa)
        _fechas(fechas, n_clasa)
        _traducciones(idiomas, n_clasa)
        _checks(checks, n_clasa)

        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_clasa), sanitize(idx), sanitize(nombre), sanitize(sanitize(nr)), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table, _join(columns), _join(values))
        data_base.query(sql)

    l = len(relacionados)
    log('Relacionados '+table, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i==l:
            log('', l, i)
        data_base.query(sql)

    for value in relaciones:
        padre, r_padre, hijo, r_hijo = value
        idr = data_base.dc_parser['id_relaciones']
        columns = [u'id', u'id_padre', u'registro_padre', u'id_hijo', u'registro_hijo', u'empresa_id']
        values = [sanitize(idr),sanitize(padre), sanitize(r_padre), sanitize(hijo), sanitize(r_hijo), empresa_id]
        data_base.dc_parser['id_relaciones'] += 1
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % ('relacionregistros', _join(columns), _join(values))
        data_base.query(sql)

def insert_zonas():
    table_db = 'zonas'
    table_erp = 'zonas'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    table_erp_sz = 'subzonas'
    ruta_rg_sz = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp_sz
    file_rg_sz = bkopen(ruta_rg_sz, huf=0)
    idxs_sz = file_rg_sz.keys()
    idxs_sz.sort()
    l_sz = len(idxs_sz)
    data_base.dc_parser[table_erp_sz] = dict()

    log(table_db, l+l_sz)

    n_zn=0
    data_base.dc_parser[table_erp] = dict()
    for idx in idxs:
        n_zn+=1
        if n_zn % 100 == 0 or n_zn==l:
            log(sanitize(idx), l, n_zn)
        data_base.dc_parser[table_erp][idx] = n_zn

        nombre, cdant =  file_rg[idx]

        columns = [u'id', u'codigo', u'nombre', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_zn), sanitize(idx), sanitize(nombre), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    relaciones = []
    nr = data_base.dc_parser['tiporegistros']['zonas']
    for idx in idxs_sz:
        n_zn+=1
        if n_zn % 100 == 0 or n_zn==l:
            log(sanitize(idx), l, n_zn)
        data_base.dc_parser[table_erp_sz][idx] = n_zn

        nombre , padre, cdant=  file_rg_sz[idx]
        if not padre or padre not in data_base.dc_parser['zonas'].keys():
            continue
        id_padre = data_base.dc_parser['zonas'][padre]
        relaciones.append([id_padre, nr, n_zn, nr])
        columns = [u'id', u'codigo', u'nombre', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_zn), sanitize(idx), sanitize(nombre), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    for value in relaciones:
        padre, r_padre, hijo, r_hijo = value
        idr = data_base.dc_parser['id_relaciones']
        columns = [u'id', u'id_padre', u'registro_padre', u'id_hijo', u'registro_hijo', u'empresa_id']
        values = [sanitize(idr),sanitize(padre), sanitize(r_padre), sanitize(hijo), sanitize(r_hijo), empresa_id]
        data_base.dc_parser['id_relaciones'] += 1
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % ('relacionregistros', _join(columns), _join(values))
        data_base.query(sql)

def insert_sociedades():
    table_db = 'sociedades'
    table_erp = 'sociedades'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    n_soc=0
    data_base.dc_parser[table_erp] = dict()
    for idx in file_rg.keys():
        n_soc+=1
        if n_soc % 100 == 0 or n_soc==l:
            log(sanitize(idx), l, n_soc)
        data_base.dc_parser[table_erp][idx] = n_soc

        nombre =  file_rg[idx][0]
        cdant=''

        columns = [u'id', u'codigo', u'nombre', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_soc), sanitize(idx), sanitize(nombre), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_tarifas():
    table_db = 'tarifas'
    table_erp = 'tarifas'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)

    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)
    n_tari=0
    data_base.dc_parser[table_erp] = dict()
    for idx in file_rg.keys():
        n_tari+=1
        if n_tari % 100 == 0 or n_tari==l:
            log(sanitize(idx), l, n_tari)
        data_base.dc_parser[table_erp][idx] = n_tari

        nombre,mar, comi, cdant=  file_rg[idx]

        columns = [u'id', u'codigo', u'nombre', u'margen', u'comision', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_tari), sanitize(idx), sanitize(nombre), sanitize(mar), sanitize(comi), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_rutas():
    table_db = 'rutas'
    table_erp = 'rutas'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    n_ruta=0
    data_base.dc_parser[table_erp] = dict()
    for idx in file_rg.keys():
        n_ruta+=1
        if n_ruta % 100 == 0 or n_ruta==l:
            log(sanitize(idx), l, n_ruta)
        data_base.dc_parser[table_erp][idx] = n_ruta
        nombre, cdant, dias =  file_rg[idx]

        columns = [u'id', u'codigo', u'nombre', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_ruta), sanitize(idx), sanitize(nombre), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_marcas():
    table_db = 'marcas'
    table_erp = 'marcas'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    n_marca=0
    data_base.dc_parser[table_erp] = dict()
    for idx in file_rg.keys():
        n_marca+=1
        if n_marca % 100 == 0 or n_marca==l:
            log(sanitize(idx), l, n_marca)

        data_base.dc_parser[table_erp][idx] = n_marca

        nombre=  file_rg[idx][0]

        columns = [u'id', u'codigo', u'nombre', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_marca), sanitize(idx), sanitize(nombre), sanitize(''), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_tipos():
    table_db = 'tipos'

    ls_t = [
        ['articulos_formatos', 16],
        ['contratos_ss',17] ,
        ['costes_descompuestos', 18],
        ['delegaciones_tipos', 11],
        ['llamadas_tipos', 12],
        ['obras_tipos', 13],
        ['tipo_gastos', 15],
        ['vehiculos_tipos', 14]
    ]

    n_tip=0

    for ln_t in ls_t:
        table_erp, tipo = ln_t
        ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
        file_rg = bkopen(ruta_rg, huf=0)
        idxs = file_rg.keys()
        idxs.sort()
        l = len(idxs)

        log(table_erp, l)

        data_base.dc_parser[table_erp] = dict()
        x = 0
        for idx in idxs:
            n_tip+=1
            x+=1
            if x % 100 == 0 or x==l:
                log(sanitize(idx), l, x)

            data_base.dc_parser[table_erp][idx] = n_tip
            nombre =  file_rg[idx][0]
            cdant=''
            columns = [u'id', u'codigo', u'nombre', u'codigo_anterior', u'orden', u'subtipo_id', u'empresa_id']
            values = [sanitize(n_tip), sanitize(idx), sanitize(nombre), sanitize(cdant), sanitize(n_tip), sanitize(tipo), empresa_id]
            sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
            data_base.query(sql)
    return n_tip

def insert_series():
    table_db = 'series'
    table_erp = 'series'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    nn=0
    data_base.dc_parser[table_erp] = dict()
    for idx in file_rg.keys():
        nn+=1
        if nn % 100 == 0 or nn==l:
            log(sanitize(idx), l, nn)

        data_base.dc_parser[table_erp][idx] = nn

        nombre, doc, obs, post=  file_rg[idx][:4]
        post_serie_ano = post=='A'
        post_serie_mes = post=='M'
        columns = [u'id', u'codigo', u'nombre', u'codigo_anterior', u'post_serie_ano', u'post_serie_mes', u'observaciones', u'empresa_id']
        values = [sanitize(nn), sanitize(idx), sanitize(nombre), sanitize(''), sanitize(post_serie_ano), sanitize(post_serie_mes), sanitize('\n'.join(obs)), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_tipos_contratos(n_tipcon):
    table_erp = 'contratos_clases'
    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)

    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_erp, l)

    data_base.dc_parser[table_erp] = dict()
    tipo = 10
    x = 0
    for idx in idxs:
        n_tipcon+=1
        x+=1
        if x % 100 == 0 or x==l:
            log(sanitize(idx), l, x)
        data_base.dc_parser[table_erp][idx] = n_tipcon

        nombre, serp, cert, n1, serc=  file_rg[idx][:5]
        cdant = ''
        columns = [u'id', u'codigo', u'nombre', u'codigo_anterior', u'orden', u'subtipo_id', u'empresa_id']
        values = [sanitize(n_tipcon), sanitize(idx), sanitize(nombre), sanitize(cdant), sanitize(n_tipcon), sanitize(tipo), empresa_id]

        if serc in data_base.dc_parser['series']:
            columns.append('serie_certificados_id')
            values.append(sanitize(data_base.dc_parser['series'][serc]))

        if serp in data_base.dc_parser['series']:
            columns.append('serie_partes_id')
            values.append(sanitize(data_base.dc_parser['series'][serp]))

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % ('tipos', _join(columns), _join(values))
        data_base.query(sql)

def insert_codigo_postal():
    table_db = 'codigopostal'
    table_erp = 'postal'

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)

    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    n_cp=0
    for idx in file_rg.keys():
        pais, pob, prv, otro, ca =  file_rg[idx]
        if len(idx) != 5:
            continue
        if idx.startswith('00'):
            continue
        if idx[:2] not in data_base.dc_parser['provincias'].keys():
            continue

        n_cp+=1
        if n_cp % 100 == 0 or n_cp==l:
            log(sanitize(idx), l, n_cp)
        data_base.dc_parser[table_erp][idx] = n_cp
        pr = data_base.dc_parser['provincias'][idx[:2]]

        columns = [u'id', u'codigo', u'provincia_id', u'poblacion', u'empresa_id']
        values = [sanitize(n_cp), sanitize(idx), sanitize(pr), sanitize(pob), empresa_id]

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_almacenes():
    table_db = 'almacenes'
    table_erp = 'almacenes'
    n_registro = data_base.dc_parser['tiporegistros'][table_db]

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)

    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    relacionados = list()
    n_alm=0
    for idx in file_rg.keys():
        n_alm+=1
        if n_alm % 100 == 0 or n_alm==l:
            log(sanitize(idx), l, n_alm)
        data_base.dc_parser[table_erp][idx] = n_alm
        nombre, cm, vh, direc, cp, pob, pro, pais, tel, mail, cdant, tipo, blo =  file_rg[idx]
        cdant=''
        if direc or cp or pob or pro:
            _direcciones(data_base, [[direc, cp, pob, pro, pais]], n_alm, n_registro, relacionados, data_base.dc_parser, True, True)

        code = tipo + '|tipo_almacen'
        id_tipo = data_base.dc_parser['opcionestipos'].get(code, data_base.dc_parser['opcionestipos']['IN|tipo_almacen'])
        columns = [u'id', u'codigo', u'nombre', u'capacidad_maxima', u'telefono', u'fax', u'email', u'codigo_anterior',
                   u'bloqueado', u'tipo_id', u'empresa_id']
        values = [sanitize(n_alm), sanitize(idx), sanitize(nombre), sanitize(cm), sanitize(tel), sanitize(''), sanitize(mail), sanitize(cdant),
                  sanitize(blo=='S'), sanitize(id_tipo), empresa_id]

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)


    l = len(relacionados)

    log('Direcciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i==l:
            log(sanitize(''), l, i)
        data_base.query(sql)

def insert_bancos():
    table_db = 'bancos'
    table_erp = 'bancos'
    data_base.dc_parser[table_erp] = dict()
    n_registro = data_base.dc_parser['tiporegistros'][table_db]

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)

    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)
    relacionados = list()
    n_ban=0
    for idx in idxs:
        n_ban+=1
        if n_ban % 100 == 0 or n_ban==l:
            log(sanitize(idx), l, n_ban)
        data_base.dc_parser[table_erp][idx] = n_ban
        (nombre, direc, pob, pro, cp, tel, fax, mail, contactos, tipo, n1, n2, n3, riesgo, ccta, obs, cdant,
         pref, n4, n5, iban, swift, suf, sep)=  file_rg[idx]
        if direc or cp or pob or pro:
            _direcciones(data_base, [[direc, cp, pob, pro, '']], n_ban, n_registro, relacionados, data_base.dc_parser, True, True)
        columns = [u'id', u'codigo', u'nombre', u'is_caja', u'telefono', u'fax', u'email', u'riesgo_total', u'iban',
                   u'prefijos_csb', u'sufijo_csb', u'separador_csb', u'observaciones', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_ban), sanitize(idx), sanitize(nombre), sanitize(tipo=='C'), sanitize(tel), sanitize(fax), sanitize(mail),
                  sanitize(riesgo), sanitize(iban), sanitize(pref), sanitize(suf), sanitize(sep), sanitize('\n'.join(obs)),
                  sanitize(cdant), empresa_id]

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    l = len(relacionados)
    log('Direcciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i==l:
            log(sanitize(''), l, i)
        data_base.query(sql)

def insert_departamentos():
    table_db = 'departamentos'
    table_erp = 'departamentos'

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    n_dep=0
    data_base.dc_parser[table_erp] = dict()
    for idx in file_rg.keys():
        n_dep+=1
        if n_dep % 100 == 0 or n_dep==l:
            log(sanitize(idx), l, n_dep)
        data_base.dc_parser[table_erp][idx] = n_dep

        nombre =  file_rg[idx][0]

        columns = [u'id', u'codigo', u'nombre', u'empresa_id']
        values = [sanitize(n_dep), sanitize(idx), sanitize(nombre), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_capitulos():

    def _idiomas(ls_idi, index):
        for cd_idi in ls_idi:
            id_idi = data_base.dc_parser['idiomas'][cd_idi]
            id_ = data_base.dc_parser['id_caracteristicas_registros']
            data_base.dc_parser['id_caracteristicas_registros'] += 1

            table_rel = 'traducciones'
            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'idioma_id': sanitize(id_idi),
                'nombre': sanitize(nombre),
                'id_registro': sanitize(index),
                'registro': sanitize(n_registro),
            }
            columns_ln = list()
            values_ln = list()
            ke = list(dc_ln.keys())
            ke.sort()
            for k in ke:
                v = dc_ln[k]
                if v is None:
                    continue
                columns_ln.append(k)
                values_ln.append(v)

            sql_ln = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_rel, _join(columns_ln), _join(values_ln))

            relacionados.append(sql_ln)

    table_db = 'capitulos'
    table_erp = 'capitulos'
    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)
    n_registro = data_base.dc_parser['tiporegistros']['capitulos']
    log(table_db, l)

    relacionados = list()
    n_cap = 0
    data_base.dc_parser[table_erp] = dict()
    for idx in file_rg.keys():
        n_cap+=1
        if n_cap % 100 == 0 or n_cap==l:
            log(sanitize(idx), l, n_cap)
        data_base.dc_parser[table_erp][idx] = n_cap

        nombre, referencia, cdant, desc, idio =  file_rg[idx]
        _idiomas(idio, n_cap)
        columns = [u'id', u'codigo', u'nombre', u'referencia', u'descripcion_extendida', u'codigo_anterior', u'empresa_id']
        values = [sanitize(n_cap), sanitize(idx), sanitize(nombre), sanitize(referencia), sanitize(desc), sanitize(cdant), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    l = len(relacionados)
    log('Relacionados '+table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i==l:
            log('', l, i)
        data_base.query(sql)

def insert_transportistas():
    table_db = 'transportistas'
    table_erp = 'transportistas'
    n_registro = data_base.dc_parser['tiporegistros'][table_db]

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)
    relacionados = list()
    n_tran = 0
    data_base.dc_parser[table_erp] = dict()
    for idx in file_rg.keys():
        n_tran+=1
        if n_tran % 100 == 0 or n_tran==l:
            log(sanitize(idx), l, n_tran)
        data_base.dc_parser[table_erp][idx] = n_tran

        nombre, cdant, aut, taut, nimo, cnae, direc, pob, cp, pro, pais, tel, email, cif =  file_rg[idx][:14]

        if direc or cp or pob or pro:
            _direcciones(data_base, [[direc, cp, pob, pro, pais]], n_tran, n_registro, relacionados, data_base.dc_parser, True, True)

        columns = [u'id', u'codigo', u'nombre', u'cif', u'telefono', u'email', u'empresa_id']
        values = [sanitize(n_tran), sanitize(idx), sanitize(nombre), sanitize(cif), sanitize(tel), sanitize(email), empresa_id]
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    l = len(relacionados)
    log('Direcciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log(sanitize(''), l, i)
        data_base.query(sql)


def insert_certificados():
    table_db = 'modeloscertificados'
    table_erp = 'partes_certificado_modelo'

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    n_mcert=0
    for idx in file_rg.keys():
        n_mcert+=1
        if n_mcert % 100 == 0 or n_mcert==l:
            log(sanitize(idx), l, n_mcert)
        data_base.dc_parser[table_erp][idx] = n_mcert
        nombre = file_rg[idx][0]

        columns = [u'id', u'codigo', u'nombre', u'empresa_id']
        values = [sanitize(n_mcert), sanitize(idx), sanitize(nombre), empresa_id]

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    table_db = 'certificados'
    table_erp = 'partes_certificado'

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    n_cert=0
    for idx in file_rg.keys():
        n_cert+=1
        if n_cert % 100 == 0 or n_cert==l:
            log(sanitize(idx), l, n_cert)
        data_base.dc_parser[table_erp][idx] = n_cert
        nada, documento, secundario = file_rg[idx]

        columns = [u'id', u'codigo', u'secundario', u'empresa_id']
        values = [sanitize(n_cert), sanitize(idx), sanitize(secundario=='S'), empresa_id]

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_formas_pago():
    table_db = 'metodospago'
    table_erp = 'fpago'

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    n_fp=0
    for idx in file_rg.keys():
        n_fp+=1
        if n_fp % 100 == 0 or n_fp==l:
            log(sanitize(idx), l, n_fp)
        data_base.dc_parser[table_erp][idx] = n_fp
        (nombre, venc, cartera, tipo, caducidad_pago, dias, recargo, cdant, cuenta, avisar, banco, imp_iban, imp_cob,
         imp_deno) = file_rg[idx]

        columns = [u'id', u'codigo', u'nombre', u'tipo_id', u'n_vencimientos', u'genera_cartera', u'caducidad_pago',
        u'recargo_financiero', u'numero_cuenta', u'aviso_cliente', u'imp_iban', u'imp_cobrado', u'imp_denominacion',
        u'codigo_anterior', u'empresa_id']
        tipo_id = data_base.dc_parser['opcionestipos'][tipo+'|tipo_cartera']
        values = [sanitize(n_fp), sanitize(idx), sanitize(nombre), sanitize(tipo_id), sanitize(venc), sanitize(cartera=='S'), sanitize(caducidad_pago),
                  sanitize(recargo), sanitize(cuenta), sanitize(avisar=='S'), sanitize(imp_iban=='S'), sanitize(imp_cob=='S'), sanitize(imp_deno=='S'),
                  sanitize(cdant), empresa_id]

        if banco and banco in data_base.dc_parser['bancos'].keys():
            banco_id = data_base.dc_parser['bancos'][banco]
            columns.append(u'banco_id')
            values.append(sanitize(banco_id))
        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

def insert_personal():
    table_db = 'personal'
    table_erp = 'personal'
    data_base.dc_parser[table_erp] = dict()
    n_registro = data_base.dc_parser['tiporegistros'][table_db]

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)
    relacionados = list()
    n_pers=0
    for idx in file_rg.keys():
        n_pers+=1
        if n_pers % 100 == 0 or n_pers==l:
            log(sanitize(idx), l, n_pers)
        data_base.dc_parser[table_erp][idx] = n_pers
        (nombre, email, domicilio, poblacion, provincia, codigo_postal, dni, observaciones, baja, fecha, motivo,
         contactos_ls, telefono_empresa, puesto, almacen_ls, numero_colegiado, rol_horario_laboral, contrasena,
         cdant, numero_seguridad_social, fecha_nacimiento, estado_civil, hijos, tipo_carnet_conducir, ccc,
         estado_actual, empresa_actual, antiguedad, fecha_alta_contrato_actual, tipo_contrato_actual, codigo_contrato_ss,
         obra_cliente_lugar, prorroga_contrato_actual, fin_del_contrato_actual, departamento, categoria, historico_categoria_puesto,
         email_empresa, coste_anual_empresa, protocolo_reconocimento_medico, fecha_ultimo_reconocimiento_medico, caducidad_reconocimiento_medico,
         restricciones_reconocimiento_medico, titulaciones_ls, homologaciones_ls, curso_prl_general, curso_prl_especifico, otros_cursos_ls,
         tarjeta_sanitaria_europea, es_vendedor, es_tecnico, es_cobrador, n1, n2,
         n3, permitir_firmar_certificados, albs_ls, facturas_ls, personal_externo, transportista, comision_general)  = file_rg[idx]


        if domicilio or codigo_postal or poblacion or provincia:
            _direcciones(data_base, [[domicilio, codigo_postal, poblacion, provincia, '']], n_pers, n_registro, relacionados, data_base.dc_parser, True, True)

        tipo_contrato_actual_id = None
        departamento_id = data_base.dc_parser['departamentos'].get(departamento)
        almacen_id = None
        if almacen_ls:
            almacen_id = data_base.dc_parser['transportistas'].get(almacen_ls[0])

        fecha = parser_date(fecha)
        fecha_nacimiento = parser_date(fecha_nacimiento)
        fecha_alta_contrato_actual = parser_date(fecha_alta_contrato_actual)
        prorroga_contrato_actual = parser_date(prorroga_contrato_actual)
        fin_del_contrato_actual = parser_date(fin_del_contrato_actual)
        fecha_ultimo_reconocimiento_medico = parser_date(fecha_ultimo_reconocimiento_medico)
        caducidad_reconocimiento_medico = parser_date(caducidad_reconocimiento_medico)
        antiguedad = parser_date(antiguedad)

        dc = {
            'almacen_id':almacen_id,
            'antiguedad':sanitize(antiguedad),
            'baja':sanitize(baja=='S'),
            'caducidad_reconocimento':caducidad_reconocimiento_medico,
            'categoria':sanitize(categoria),
            'cobrador':sanitize(es_cobrador=='S'),
            'codigo':sanitize(idx),
            'codigo_anterior':sanitize(cdant),
            'codigo_contrato_ss':sanitize(codigo_contrato_ss),
            'comision_general':sanitize(comision_general),
            'contrasena':sanitize(contrasena),
            'coste_anual_empresa':sanitize(coste_anual_empresa),
            'curso_prl_especifico':sanitize(curso_prl_especifico),
            'curso_prl_general':sanitize(curso_prl_general),
            'departamento_id':sanitize(departamento_id),
            'dni':sanitize(dni),
            'email':sanitize(email),
            'email_empresa':sanitize(email_empresa),
            'empresa_actual':sanitize(empresa_actual),
            'estado_actual':sanitize(estado_actual),
            'estado_civil':sanitize(estado_civil),
            'fecha_alta_contrato':sanitize(fecha_alta_contrato_actual),
            'fecha_baja':sanitize(fecha),
            'fecha_nacimiento':sanitize(fecha_nacimiento),
            'fecha_ultimo_reconocimento':sanitize(fecha_ultimo_reconocimiento_medico),
            'fin_del_contrato_actual':sanitize(fin_del_contrato_actual),
            'hijos':sanitize(hijos),
            'historico_categoria':sanitize(historico_categoria_puesto),
            'iban':sanitize(ccc),
            'motivo_baja':sanitize(motivo),
            'nombre':sanitize(nombre),
            'numero_colegiado':sanitize(numero_colegiado),
            'numero_ss':sanitize(numero_seguridad_social),
            'obra_cliente_lugar':prorroga_contrato_actual,
            'observaciones':sanitize(observaciones),
            'permitir_firmar_cert':sanitize(permitir_firmar_certificados=='S'),
            'personal_externo':sanitize(personal_externo=='S'),
            'prorroga_contrato_actual':sanitize(prorroga_contrato_actual),
            'protocolo_reconocimento':sanitize(protocolo_reconocimento_medico),
            'puesto':sanitize(puesto),
            'restricciones_reconocimento':sanitize(restricciones_reconocimiento_medico),
            'tarjeta_sanitaria_eu':sanitize(tarjeta_sanitaria_europea),
            'tecnico':sanitize(es_tecnico=='S'),
            'telefono_empresa':sanitize(telefono_empresa),
            'telefono_personal':sanitize(''),
            'tipo_carnet_conducir':sanitize(tipo_carnet_conducir),
            'tipo_contrato_actual_id': tipo_contrato_actual_id,
            'vendedor':sanitize(es_vendedor=='S')
        }
        columns = [u'id', u'empresa_id']
        values = [sanitize(n_pers), '1']
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

    l = len(relacionados)
    log('Direcciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log(sanitize(''), l, i)
        data_base.query(sql)

def insert_tipos_partes():
    table_db = 'partestipos'
    table_erp = 'partes_tipos'

    data_base.dc_parser[table_erp] = dict()

    ruta_rg = "D:/Server/Companies/jonathan/tesein/EJA/" + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = file_rg.keys()
    idxs.sort()
    l = len(idxs)

    log(table_db, l)

    tecnicos = []
    n_tparte=0
    for idx in file_rg.keys():
        n_tparte+=1
        if n_tparte % 100 == 0 or n_tparte==l:
            log(sanitize(idx), l, n_tparte)
        data_base.dc_parser[table_erp][idx] = n_tparte
        nombre, cdar, taccion, cert, tec = file_rg[idx][:5]

        columns = [u'id', u'codigo', u'nombre', u'empresa_id']
        values = [sanitize(n_tparte), sanitize(idx), sanitize(nombre), empresa_id]
        if taccion and taccion in data_base.dc_parser['acciones_tipos'].keys():
            taccion_id = data_base.dc_parser['acciones_tipos'][taccion]
            columns.append(u'tipo_accion_id')
            values.append(sanitize(taccion_id))
        else:
            columns.append(u'tipo_accion_id')
            values.append(sanitize(18))
        reg = data_base.dc_parser['tiporegistros']['partestipos']
        if tec:
            for t in tec:
                t = data_base.dc_parser['personal'][t]
                n_ = data_base.dc_parser['id_tecnicos']
                columnst = [u'id', u'tecnico_id', u'defecto', u'id_registro', u'registro', 'empresa_id']
                valuest = [sanitize(n_), sanitize(t), sanitize(False), sanitize(n_tparte), sanitize(reg), empresa_id]
                sq = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % ('tecnicosregistros', _join(columnst), _join(valuest))
                tecnicos.append(sq)
                data_base.dc_parser['id_tecnicos'] += 1

        sql = u"INSERT INTO empresas_%s(%s) VALUES (%s)" % (table_db, _join(columns), _join(values))
        data_base.query(sql)

    for sql in tecnicos:
        data_base.query(sql)


if __name__ == '__main__':
    data_base = Database(1)
    empresa_id = u'1'
    data_base.reset()

    insert_empresa()
    insert_paises()
    insert_provincias()
    insert_tipos_registros()
    insert_sub_tipos_registros()
    insert_divisas()
    insert_idiomas()
    insert_clasificacionclientes()
    insert_caracteristica()
    insert_opciones_tipos()
    insert_fechas()
    insert_normativa()
    insert_checklists()
    insert_tipos_acciones()
    insert_clasificacionarticulos()
    insert_zonas()
    insert_sociedades()
    insert_tarifas()
    insert_rutas()
    insert_marcas()
    n = insert_tipos()
    insert_series()
    insert_tipos_contratos(n)
    insert_codigo_postal()
    insert_administradores()
    insert_almacenes()
    insert_bancos()
    insert_departamentos()
    insert_capitulos()
    insert_transportistas()
    insert_certificados()
    insert_formas_pago()
    insert_personal()
    insert_tipos_partes()

    data_base.close()
