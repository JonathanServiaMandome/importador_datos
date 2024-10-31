# -*- coding: utf-8 -*-
import codecs
import json
import os

import pyodbc

from functions import Num_aFecha, stripRtf, bkopen

PATH = 'D:/Server/Companies/jonathan/tesein/EJA/'
empresa_id = u'1'
reset = True

class Database:
    host = "127.0.0.1"
    user = "postgres"
    passwd = "admin"
    db = "sat_db"

    def __init__(self, level, remote=False):
        if remote:
            self.connection = pyodbc.connect(
                "DRIVER={PostgreSQL Unicode(x64)}; "
                "DATABASE=django_pruebas;     "
                "UID=postgres; "
                "PWD=Vay1234..; "
                "SERVER=157.230.110.33; "
                "PORT=5432;",
                autocommit=True)
        else:
            self.connection = pyodbc.connect(
                "DRIVER={PostgreSQL Unicode(x64)}; "
                "DATABASE=sat_db;     "
                "UID=postgres; "
                "PWD=admin; "
                "SERVER=localhost; "
                "PORT=5432;",
                autocommit=True)

        self.level  = level
        self.cursor = self.connection.cursor()
        self.dc_db = dict()
        self.inserts = list()
        self.deletes = list()

        if level > 1:

            path=os.getcwd()+'\\json\\import%d.json' % (level - 1)
            print path
            js = open(path, 'rb').read()
            self.dc_db = json.loads(js)
        self.dc_db['relaciones_faltan'] = self.dc_db.get('relaciones_faltan', list())

    def close(self):
        path= os.getcwd() + '\\json\\'
        if not os.path.isdir(path):
            os.makedirs(path)
        path +='import%d.json' % self.level
        codecs.open(path, 'w', encoding="utf-8").write(json.dumps(self.dc_db, indent=4, encoding='latin-1'))
        '''
        path=os.getcwd() +'\\import%d.sql' % self.level
        tx = '\n'.join(self.deletes)
        tx += '\n'
        tx += '\n'.join(self.inserts)
        codecs.open(path, 'w', encoding="utf-8").write(tx)
        '''


    def reset(self):
        cursor = self.cursor
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        for ln in cursor.fetchall():
            table = ln[0]
            if not table.startswith('empresas'):
                continue

            sqls = [
                'ALTER TABLE %s DISABLE TRIGGER ALL;' % table.lower(),
                'DELETE FROM %s;' % table.lower(),
                'ALTER TABLE %s ENABLE TRIGGER ALL;' % table.lower()
            ]
            for sql in sqls:
                cursor.execute(sql)
                self.deletes.append(sql)

    def select(self, q):
        cursor = self.cursor
        try:
            cursor.execute("SELECT * FROM empresas_" + q)
        except pyodbc.ProgrammingError as e:
            print(q)
            raise ValueError(e)

        return cursor.fetchall()

    def query(self, q):
        cursor = self.cursor
        try:
            cursor.execute(q)
        except pyodbc.IntegrityError as e:
            print(q)
            raise ValueError(str(e))
        except pyodbc.ProgrammingError as e:
            print(q)
            raise ValueError(str(e))
        except pyodbc.DataError as e:
            print(q)
            raise ValueError(str(e))
        except Exception as e:
            print(q)
            raise ValueError(str(e))
        self.inserts.append(q)
        return cursor

    def delete_all(self, table):
        cursor = self.cursor
        try:
            sqls = [
                'ALTER TABLE empresas_%s DISABLE TRIGGER ALL;' % table.lower(),
                'DELETE FROM empresas_%s;' % table.lower(),
                'ALTER TABLE empresas_%s ENABLE TRIGGER ALL;' % table.lower()
            ]
            for sql in sqls:
                cursor.execute(sql)
                self.deletes.append(sql)

        except pyodbc.IntegrityError as e:
            self.close()
            raise ValueError(e)

    def delete_where(self, table, where):
        cursor = self.cursor
        try:
            sqls = [
                'ALTER TABLE empresas_%s DISABLE TRIGGER ALL;' % table.lower(),
                'DELETE FROM empresas_%s WHERE %s;' % (table.lower(), where),
                'ALTER TABLE empresas_%s ENABLE TRIGGER ALL;' % table.lower()
            ]
            for sql in sqls:
                cursor.execute(sql)
                self.deletes.append(sql)

        except pyodbc.IntegrityError as e:
            self.close()
            raise ValueError(e)


    def insert(self, table, columnas, valores):
        q = u"INSERT INTO empresas_%s (%s) values (%s)" % (table.lower(), ', '.join(columnas), ', '.join(valores))
        self.cursor.execute(q)

    def __del__(self):
        self.connection.close()

def sanitize(text):
    if text is None:
        return text
    elif type(text) in (int, float, bool):
        return str(text)
    elif type(text) == str:
        text = text.decode('iso-8859-1')
    elif type(text) == type(u''):
        pass
    else:
        raise ValueError("Tipo + " + str(type(text)))
    text = "'"+text.replace("'", "''").replace('"', '""')+"'"
    return text

def i_join(ls):
    text = u''
    for l in ls:
        try:
            text += l + u', '
        except TypeError as e:
            print(ls)
            raise ValueError(e)
        except Exception as e:
            raise ValueError(e)
    return text[:-2]

def log(*args):
    if len(args)==2:
        print("Importando %d %s" % (args[1], args[0]))
    elif len(args)==3:
        print("\t - %s (%d - %d)..." % (args[0], args[2], args[1]))
    else:
        print(args)

def parser_date(date):
    date = Num_aFecha(date)
    if date is not None:
        d, m, y = date.split('/')
        date = y + '-' + m + '-' + d
    return date

def get_id(db, table):
    key = 'id_%s' % table
    db.dc_db[key] = db.dc_db.get(key, 1)
    id_ = db.dc_db[key]
    db.dc_db[key] += 1
    return id_

def dict_to_sql(data_base, dc, table, insert=True):
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

    sql = u"INSERT INTO empresas_%s(%s) VALUES (%s);" % (table, i_join(columns), i_join(values))
    if insert:
        data_base.query(sql)
    else:
        return sql

def get_data(data_base, table_db, table_erp=''):
    if not table_erp:
        table_erp = table_db
    ruta_rg = PATH + table_erp
    file_rg = bkopen(ruta_rg, huf=0)
    idxs = list(file_rg.keys())
    idxs.sort()
    l = len(idxs)

    data_base.dc_db[table_erp] = dict()
    n_registro = data_base.dc_db['tiporegistro'][table_db]


    return table_db, table_erp, file_rg, idxs, l, n_registro

def i_fechas(data_base, ls_fechas, index, n_registro, relacionados):
    table_rel = 'fecharegistro'
    for ln_fec in ls_fechas:
        if len(ln_fec)==3:
            cd_fec, periodicidad_f, tipo_periodicidad_f = ln_fec
            proxima_fecha = None
            ultima_fecha = None
        else:
            cd_fec, ultima_fecha, periodicidad_f, tipo_periodicidad_f, proxima_fecha = ln_fec

            ultima_fecha = parser_date(ultima_fecha)
            proxima_fecha = parser_date(proxima_fecha)
        if not cd_fec:
            continue
        id_fec = data_base.dc_db['nseries_fechas'].get(cd_fec)
        if id_fec is None:
            raise ValueError(ls_fechas)
        
        id_ = get_id(data_base, table_rel)
        
        tipo_periodicidad_f = data_base.dc_db['opciontipo'].get(tipo_periodicidad_f + '|tipo_periodicidad')
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'fecha_id': sanitize(id_fec),
            'periodicidad': sanitize(periodicidad_f),
            'tipo_periodicidad_id': sanitize(tipo_periodicidad_f),
            'id_registro': sanitize(index),
            'ultima_fecha': sanitize(ultima_fecha),
            'proxima_fecha': sanitize(proxima_fecha),
            'registro': sanitize(n_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_caracteristicas(data_base, ls_car, index, n_registro, relacionados, rgs_caracteristicas):
    table_rel = 'caracteristicaregistro'
    for cd_car in ls_car:
        if type(cd_car)==str:
            if rgs_caracteristicas.get(cd_car) is None:
                raise ValueError(cd_car, rgs_caracteristicas.keys())
            rg_car = rgs_caracteristicas[cd_car]
            tipo_valor = rg_car[1][0].upper()
            texto, numerico, fecha = None, None, None
        else:
            cd_car, tipo_valor, texto, numerico, fecha = cd_car[:5]

        id_car = data_base.dc_db['nseries_caracteristica'].get(cd_car)
        if id_car is None:
            continue
            
        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'caracteristica_id': sanitize(id_car),
            'tipo_valor': sanitize(tipo_valor[0].upper()),
            'id_registro': sanitize(index),
            'valor_fecha': sanitize(parser_date(fecha)),
            'valor_texto': sanitize(texto),
            'valor_numerico': sanitize(numerico),
            'registro': sanitize(n_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_tecnicos(data_base, ls_tec, index, n_registro, relacionados):
    table_rel = 'tecnicoregistro'
    for cd_tec in ls_tec:
        defecto = True
        if type(cd_tec) == list:
            cd_tec = cd_tec[0]

        id_tec = data_base.dc_db['personal'].get(cd_tec)
        if id_tec is None:
            continue
            
        id_ = get_id(data_base, table_rel)

        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'tecnico_id': sanitize(id_tec),
            'defecto': sanitize(defecto),
            'id_registro': sanitize(index),
            'registro': sanitize(n_registro),
        }

        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_traducciones(data_base, ls_tra, index, n_registro, relacionados, ext=False):
    if index==18:
        return
    table_rel = 'traduccion'
    for ln_tra in ls_tra:
        cd_tra, nom = ln_tra[:2]
        if ext:
            nom = stripRtf(nom)
        try:
            id_tra = data_base.dc_db['idiomas'][cd_tra]
        except:
            raise ValueError(ls_tra, ext, index)

        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'idioma_id': sanitize(id_tra),
            'nombre': sanitize(nom),
            'id_registro': sanitize(index),
            'registro': sanitize(n_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_checks(data_base, ls_check, index, n_registro, relacionados):
    table_rel = 'checklistregistro'
    for ln_check in ls_check:
        cd_check, tp_accion, idio = ln_check
        id_check = data_base.dc_db['checklists'][cd_check]
        id_idi = data_base.dc_db['idiomas'].get(idio, data_base.dc_db['idiomas']['001'])

        tipo_accion_id = data_base.dc_db['acciones_tipos'].get(tp_accion, data_base.dc_db['acciones_tipos'].get('RV'))

        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'checklist_id': sanitize(id_check),
            'tipo_accion_id': sanitize(tipo_accion_id),
            'idioma_id': sanitize(id_idi),
            'id_registro': sanitize(index),
            'registro': sanitize(n_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_direcciones(data_base, ls_direcciones, index, num_registro, relacionados, facturacion, envio):
    table_rel = 'direccion'
    for ln_direccion in ls_direcciones:
        domicilio, codigo_postal, poblacion, provincia, pais = ln_direccion[:5]

        id_ = get_id(data_base, table_rel)
        
        provincia = data_base.dc_db['provincia'].get(codigo_postal[:2])
        codigo_postal = data_base.dc_db['postal'].get(codigo_postal)
        pais = data_base.dc_db['paises'].get(pais)
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'direccion': sanitize(domicilio),
            'codigo_postal_id': sanitize(codigo_postal),
            'poblacion': sanitize(poblacion),
            'provincia_id': sanitize(provincia),
            'pais_id': sanitize(pais),
            'id_registro': sanitize(index),
            'facturacion': sanitize(facturacion),
            'envio': sanitize(envio),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_contactos(data_base, ls_contactos, index, num_registro, relacionados):
    table_rel = 'contacto'
    for ln_contacto in ls_contactos:
        if len(ln_contacto) == 7:
            nombre, cargo, telefono, email, defe, cif, fax = ln_contacto[:7]
            defe = defe=='S'
        else:
            nombre, cargo, telefono, email = ln_contacto[:4]
            defe = False
            cif = None
            fax = None

        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'contacto': sanitize(nombre),
            'cargo': sanitize(cargo),
            'telefono': sanitize(telefono),
            'email': sanitize(email),
            'fax': sanitize(fax),
            'cif': sanitize(cif),
            'defecto': sanitize(defe=='S'),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_iban(data_base, ls_contactos, index, num_registro, relacionados):
    table_rel = 'ibanregistro'
    for ln_contacto in ls_contactos:
        iban, swift, defe = ln_contacto[:3]

        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'iban': sanitize(iban),
            'defecto': sanitize(defe=='S'),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_relacion(data_base, ls, tabla_hijo, padre, p_registro, relacionados, tabla_d=''):
    table_rel = 'relacionregistro'
    h_registro = data_base.dc_db['tiporegistro'][tabla_hijo]
    if tabla_d:
        tabla_hijo = tabla_d

    for ln in ls:
        if not ln:
            continue
        idh = None
        if type(ln) == str:
            try:
                idh = data_base.dc_db[tabla_hijo].get(ln)
            except KeyError:
               raise ValueError(str((tabla_hijo,data_base.dc_db.keys())))
            if idh is None:
               raise ValueError(str((ln, tabla_hijo, data_base.dc_db[tabla_hijo].keys())))
        elif type(ln) == list:
           idh = data_base.dc_db[tabla_hijo].get(ln[0])
           if idh is None:
               raise ValueError(str(ln))
        if idh is None:
            continue

        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'registro_hijo': sanitize(h_registro),
            'id_hijo': sanitize(idh),
            'registro_padre': sanitize(p_registro),
            'id_padre': sanitize(padre),
            #'defecto': sanitize(defecto),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_modeloscertificado(data_base, ls_mod, index, num_registro, relacionados):
    table_rel = 'modelocertificadoregistro'
    for ln_mod in ls_mod:
        idioma, tipo_contrato, tipo_parte, modelo_certificado = ln_mod
        idioma = data_base.dc_db['idiomas'].get(idioma)
        modelo_certificado = data_base.dc_db['partes_certificado_modelo'].get(modelo_certificado)
        tipo_parte = data_base.dc_db['partes_tipos'].get(tipo_parte)
        tipo_contrato = data_base.dc_db['contratos_clases'].get(tipo_contrato)


        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'idioma_id': sanitize(idioma),
            'modelo_certificado_id': sanitize(modelo_certificado),
            'tipo_parte_id': sanitize(tipo_parte),
            'tipo_contrato_id': sanitize(tipo_contrato),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_checksecundario(data_base, ls_mod, index, num_registro, relacionados):
    table_rel = 'checklistsecundarioregistro'
    for ln_mod in ls_mod:
        idioma, modelo, checksec = ln_mod
        checklists_secundario = data_base.dc_db['checklists'].get(checksec)
        modelo_certificado = data_base.dc_db['partes_certificado_modelo'].get(modelo)
        idioma_i = data_base.dc_db['idiomas'].get(idioma)

        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'idioma_id': sanitize(idioma_i),
            'modelo_certificado_id': sanitize(modelo_certificado),
            'checklist_secundario_id': sanitize(checklists_secundario),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_precioespecial(data_base, ls_ofertas, index, num_registro, relacionados):
    table_rel = 'precioespecial'
    for ln_oferta in ls_ofertas:
        articulo, dto1, dto2, dto3, dto4, precio_venta = ln_oferta
        articulo_i = data_base.dc_db['articulos'].get(articulo)
        if articulo_i is None:
            continue
            
        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'articulo_id': sanitize(articulo_i),
            'precio': sanitize(precio_venta),
            'descuento_1': sanitize(dto1),
            'descuento_2': sanitize(dto2),
            'descuento_3': sanitize(dto3),
            'descuento_4': sanitize(dto4),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))


def i_vendedor(data_base, ls_vendedores, index, num_registro, relacionados):
    table_rel = 'vendedorregistro'
    for ln_vendedor in ls_vendedores:
        vendedor, defecto = ln_vendedor
        vendedor_i = data_base.dc_db['personal'].get(vendedor)

        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'vendedor_id': sanitize(vendedor_i),
            'defecto': sanitize(defecto=='S'),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))


def i_bases(data_base, ls_bases, index, num_registro, relacionados, ops=None):
    if ops is None:
        ops = dict()
    table_rel = 'baseimponible'
    for ln_bases in ls_bases:
        if len(ln_bases) ==4:
            base_imponible, tipo_iva, iva, recargo_equivalencia = ln_bases
            op=None
        else:
            op, base_imponible, tipo_iva, iva, recargo_equivalencia = ln_bases
            op = ops.get(op)
            
        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'impuestos': sanitize(iva),
            'base_imponible': sanitize(base_imponible),
            'recargo_equivalencia': sanitize(recargo_equivalencia),
            'tipo_iva': sanitize(tipo_iva),
            'opcion_id': sanitize(op),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))


def i_iva(data_base, ls_iva, index, num_registro, relacionados):
    table_rel = 'ivaregistro'
    for i in range(len(ls_iva)):
        p_iva, p_rec, p_igic = ls_iva[i]

        id_ = get_id(data_base, table_rel)
        
        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'porcentaje_iva': sanitize(p_iva),
            'porcentaje_retencion': sanitize(p_rec),
            'tipo_iva': sanitize(i),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))


def i_vencimientos(data_base, ls_venc, index, num_registro, relacionados):
    table_rel = 'vencimiento'
    for i in range(len(ls_venc)):
        fecha, importe = ls_venc[i][:2]

        id_ = get_id(data_base, table_rel)

        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'fecha': sanitize(parser_date(fecha)),
            'importe': sanitize(importe),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))


def i_conceptos(data_base, ls_conceptos, index, num_registro, relacionados):
    table_rel = 'facturaconcepto'
    for i in range(len(ls_conceptos)):
        concepto, tipo_iva, importe = ls_conceptos[i][:3]

        id_ = get_id(data_base, table_rel)

        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'concepto': sanitize(concepto),
            'tipo_iva': sanitize(tipo_iva),
            'importe': sanitize(importe),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))

def i_albaranes(data_base, ls_albaranes, index, num_registro, relacionados, reg='albarancompra'):
    table_rel = 'facturaalbaran'
    for i in range(len(ls_albaranes)):
        albaran = ls_albaranes[i]
        albaran = data_base.dc_db['tiporegistro'][reg]
        registro_albaran = data_base.dc_db['tiporegistro'][reg]
        
        id_ = get_id(data_base, table_rel)

        dc = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'id_albaran': sanitize(albaran),
            'registro_albaran': sanitize(registro_albaran),
            'id_factura': sanitize(index),
            'registro_factura': sanitize(num_registro),
        }
        relacionados.append(dict_to_sql(data_base, dc, table_rel, False))


def insert_empresa(data_base):
    table_db = 'empresa'

    campos_db = [u'id', u'nombre', u'identificador_fiscal', u'fecha_creacion', u'activo', u'domicilio', u'telefono']
    values_db = [u'1', u"'SAT'", u"'00000A'", u"'01/10/2024'", u"True", u"'Calle'", u"'981-999-999'"]

    log(table_db, 1)
    sql = u"INSERT INTO empresas_%s (%s) values (%s)" % (table_db, ', '.join(campos_db), ', '.join(values_db))
    data_base.query(sql)


def insert_ivacuota(data_base):
    table_db = 'ivacuota'

    values_db = [
        [21, 5.2, 0, u'01/09/2012', u'31/12/2050'],
        [10, 1.4, 1, u'01/09/2012', u'31/12/2050'],
        [4, 0.5, 2, u'01/09/2012', u'31/12/2050'],
        [0, 0, 3, u'01/09/2012', u'31/12/2050'],
    ]

    l = len(values_db)
    log(table_db, l)
    data_base.dc_db[table_db] = dict()
    n_iva = 0
    for ln in values_db:
        n_iva += 1
        iva, rec, tipo, desde, hasta = ln
        if n_iva % 100 == 0 or n_iva == l:
            log("Tipo de IVA: %d" % tipo, l, n_iva)

        data_base.dc_db[table_db][tipo] = n_iva

        dc_pais = {
            u'id': sanitize(n_iva),
            u'porcentaje_iva': sanitize(iva),
            u'porcentaje_retencion': sanitize(rec),
            u'tipo_iva': sanitize(tipo),
            u'desde_fecha': sanitize(desde),
            u'hasta_fecha': sanitize(hasta),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_pais, table_db)


def insert_modulos(data_base):
    table_db = 'modulo'
    table_db_activo = 'moduloactivo'
    data_base.dc_db[table_db] = dict()
    tipos_registros = [
        u'base',
        u'obras',
        u'sat',
        u'presupuestos_pro',
        u'crm',
        u'contabilidad',
        u'partes',
    ]

    l = len(tipos_registros)
    log(table_db, l)
    for x in range(l):
        n_tipo = x + 1

        nombre = tipos_registros[x]
        log(nombre, l, n_tipo)
        dc = {
            u'id': sanitize(n_tipo),
            u'nombre': sanitize(nombre),
        }
        data_base.dc_db[table_db][nombre] = x + 1
        dict_to_sql(data_base, dc, table_db)
        activo = True
        if nombre in [u'contabilidad', u'crm', u'obras']:
            activo = False
        dc = {
            u'id': sanitize(n_tipo),
            u'modulo_id': sanitize(n_tipo),
            u'activo': sanitize(activo),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc, table_db_activo)


def insert_menus(data_base):
    table_db = 'menu'
    data_base.dc_db[table_db] = dict()
    tipos_registros = [
        u'Maestros',
        u'Ventas',
        u'Compras',
        u'Almacén',
        u'Obras',
        u'SAT',
        u'CRM',
        u'Contabilidad',
        u'Apps',
        u'Utilidades',
    ]

    l = len(tipos_registros)
    log(table_db, l)
    for x in range(l):
        n_tipo = x + 1

        nombre = tipos_registros[x]
        log(nombre, l, n_tipo)
        dc = {
            u'id': sanitize(n_tipo),
            u'nombre': sanitize(nombre),
        }
        data_base.dc_db[table_db][nombre] = x + 1
        dict_to_sql(data_base, dc, table_db)


def insert_tipos_registros(data_base):
    table_db = 'tiporegistro'
    data_base.dc_db[table_db] = dict()
    tipos_registros = [
        [u'Recuento', u'Recuentos de mercancía', u'Almacén', u'base'],
        [u'Inventario', u'Inventario de mercancía', u'Almacén', u'base'],
        [u'Trasvase', u'Trasvases de mercancía', u'Almacén', u'base'],
        [u'PedidoAlmacen', u'Pedidos almacén', u'Almacén', u'base'],
        [u'PeticionPrecios', u'Petición de precios', u'Compras', u'base'],
        [u'PedidoProveedor', u'Pedidos a proveedor', u'Compras', u'base'],
        [u'AlbaranCompra', u'Albarán de compra', u'Compras', u'base'],
        [u'FacturaRecibida', u'Facturas recibidas', u'Compras', u'base'],
        [u'Pago', u'Pagos a proveedores', u'Compras', u'base'],
        [u'PagoAlbaraneCompra', u'Pagos de albaranes de compra', u'Compras', u'base'],
        [u'RemesaPago', u'Remesas de pagos', u'Compras', u'base'],
        [u'Presupuesto', u'Presupuestos', u'Ventas', u'base'],
        [u'PedidoCliente', u'Pedidos a cliente', u'Ventas', u'base'],
        [u'AlbaranVenta', u'Albarán de venta', u'Ventas', u'base'],
        [u'FacturaEmitida', u'Facturas emitidas', u'Ventas', u'base'],
        [u'Cobro', u'Cobros a clientes', u'Ventas', u'base'],
        [u'CobroAlbaranVenta', u'Cobros de albaranes de venta', u'Ventas', u'base'],
        [u'RemesaCobro', u'Remesas de cobros', u'Ventas', u'base'],
        [u'Contrato', u'Contratos', u'SAT', u'sat'],
        [u'Llamada', u'Llamadas', u'SAT', u'sat'],
        [u'Arqueo', u'Arqueos', u'SAT', u'sat'],
        [u'Obra', u'Obras e instalaciones', u'Obras', u'obras'],
        [u'OrdenTrabajo', u'Órdenes de trabajo', u'SAT', u'partes'],
        [u'Trabajos', u'Trabajos a realizar', u'SAT', u'partes'],
        [u'ArticuloCompuesto', u'Artículos compuestos', u'Obras', u'obras'],
        [u'Elemento', u'Elementos revisables', u'SAT', u'sat'],
        [u'Vehiculo', u'Vehículos', u'Maestros', u'base'],
        [u'Delegacion', u'Delegaciones de clientes', u'Maestros', u'base'],
        [u'Cliente', u'Clientes', u'Maestros', u'base'],
        [u'Proveedor', u'Proveedores', u'Maestros', u'base'],
        [u'Empleado', u'Empleados', u'Maestros', u'base'],
        [u'ModoPago', u'Metodos de pago', u'Maestros', u'base'],
        [u'Fecha', u'Fechas recalculables', u'SAT', u'sat'],
        [u'CodigoPostal', u'Codigos postales', u'Maestros', u'base'],
        [u'TipoOrdenTrabajo', u'Tipos de órdenes de trabajo', u'SAT', u'partes'],
        [u'TipoTrabajo', u'Tipos de trabajos', u'SAT', u'partes'],
        [u'Articulo', u'Artículos', u'Maestros', u'base'],
        [u'Almacen', u'Almacenes', u'Maestros', u'base'],
        [u'Tipo', u'Tipos de registro', u'Maestros', u'base'],
        [u'Serie', u'Series', u'Maestros', u'base'],
        [u'Provincia', u'Provincias', u'Maestros', u'base'],
        [u'Region', u'Regiones', u'Maestros', u'base'],
        [u'Pais', u'Paises', u'Maestros', u'base'],
        [u'Checklist', u'Checklists', u'Maestros', u'base'],
        [u'Banco', u'Bancos', u'Maestros', u'base'],
        [u'Normativa', u'Normativas para certificados', u'SAT', u'sat'],
        [u'NormativaCapitulo', u'Capítulos de normativa para certificados', u'SAT', u'sat'],
        [u'Zona', u'Zonas', u'Maestros', u'base'],
        [u'Transportista', u'Transportistas', u'Maestros', u'base'],
        [u'Tarifa', u'Tarifas', u'Maestros', u'base'],
        [u'Swift', u'Swift', u'Maestros', u'base'],
        [u'Sociedad', u'Sociedades', u'Maestros', u'base'],
        [u'Ruta', u'Rutas', u'Maestros', u'base'],
        [u'Certificado', u'Certificados', u'SAT', u'sat'],
        [u'ModeloCertificado', u'Modelos de certificados', u'SAT', u'sat'],
        [u'MarcaRiesgo', u'Marcas de riesgo', u'Maestros', u'base'],
        [u'Marca', u'Marcas', u'SAT', u'sat'],
        [u'Idioma', u'Idiomas', u'Maestros', u'base'],
        [u'Email', u'Emails', u'Maestros', u'base'],
        [u'Divisa', u'Divisas', u'Maestros', u'base'],
        [u'Departamento', u'Departamentos', u'Maestros', u'base'],
        [u'ClasificacionCliente', u'Clasificacion de los clientes', u'Maestros', u'base'],
        [u'ClasificacionArticulo', u'Clasificacion de articulos', u'Maestros', u'base'],
        [u'CosteEmpleado', u'Costes empleados', u'SAT', u'sat'],
        [u'Capitulo', u'Capitulos', u'SAT', u'sat'],
        [u'Caracteristica', u'Caracteristicas de los elementos', u'SAT', u'sat'],
        [u'Administrador', u'Administradores de fincas', u'Maestros', u'base'],
        [u'ValorPredefinido', u'Valores predefinidos checklists', u'Maestros', u'base'],
        [u'Texto', u'Textos para las impresiones', u'Maestros', u'base'],
    ]

    l = len(tipos_registros)
    log(table_db, l)
    for x in range(l):
        n_tipo = x + 1
        registro, nombre, menu, modulo = tipos_registros[x]
        registro = registro.lower()
        if n_tipo % 100 == 0 or n_tipo == l:
            log(registro, l, n_tipo)
        modulo = data_base.dc_db['modulo'][modulo]
        menu = data_base.dc_db['menu'][menu]
        dc = {
            u'id': sanitize(n_tipo),
            u'registro': sanitize(registro),
            u'nombre': sanitize(nombre),
            u'modulo_id': sanitize(modulo),
            u'menu_id': sanitize(menu),
            u'empresa_id': empresa_id
        }
        data_base.dc_db[table_db][registro] = x + 1
        dict_to_sql(data_base, dc, table_db)


def insert_sub_tipos_registros(data_base):
    table_db = 'subtiporegistro'
    data_base.dc_db[table_db] = dict()
    tipos_registros = [
        [u'actividad', u'Actividad empresarial'],  # 1 -
        [u'categoria_cliente', u'Categoría empresarial'],  # 2 -
        [u'sector', u'Sector empresarial'],  # 3 -
        [u'sub_sector', u'Sub sectores empresarial'],  # 4 -
        [u'categoria_articulo', u'Categoría artículo'],  # 5 -
        [u'grupo', u'Grupo artículo'],  # 6 -
        [u'familia', u'Familia artículo'],  # 7 -
        [u'sub_familia', u'Sub familia artículo'],  # 8 -
        [u'tipo_almacen', u'Tipos de almacén'],  # 9 -
        [u'tipo_contrato', u'Tipos de contratos'],  # 10 -
        [u'tipo_delegacion', u'Tipos de delegaciones'],  # 11 -
        [u'tipo_llamada', u'Tipos de llamadas'],  # 12 -
        [u'tipo_obra', u'Tipos de obra'],  # 13 -
        [u'tipo_vehiculo', u'Tipos de vehículos'],  # 14 -
        [u'tipo_gasto', u'Tipos de gastos'],  # 15 -
        [u'formato_articulo', u'Formatos de los artículos'],  # 16 -
        [u'contrato_ss', u'Tipos de contratos de la seguridad social'],  # 17 -
        [u'coste_descompuesto', u'Tipos de costes de los descompuestos'],  # 18 -
        [u'estado_parte', u'Estados de los partes'],  # 19 -
        [u'estado_cartera', u'Estados de la cartera'],  # 20 -
        [u'estado_obra', u'Estados de las obras'],  # 21 -
        [u'estado_trasvase', u'Estados de los trasvases'],  # 22 -
        [u'estado_pedido', u'Estados de los pedidos'],  # 23 -
        [u'estado_presupuesto', u'Estados de los Presupuestos'],  # 24 -
        [u'tipo_cartera', u'Tipos de los recibos de cartera'],  # 25 -
        [u'tipo_facturacion_auto', u'Tipos de facturación automática'],  # 26 -
        [u'tipo_albaran_venta', u'Tipos de los albaranes de venta'],  # 27 -
        [u'tipo_albaran_compra', u'Tipos de los albaranes de compra'],  # 28 -
        [u'tipo_compra_proveedor', u'Tipos de las compras del proveedor'],  # 29 -
        [u'tipo_proveedor', u'Tipos de los Proveedores'],  # 30 -
        [u'tipo_iva_cliente', u'Tipos de IVA del cliente '],  # 31 -
        [u'tipo_iva_proveedor', u'Tipos de IVA del proveedor '],  # 32 -
        [u'tipo_periodicidad', u'Tipos de periodicidades '],  # 33 -
        [u'tipo_articulo', u'Tipos de artículos'],  # 34 -
        [u'tipo_marcas_serie', u'Tipos de las marcas de serie'],  # 35 -
        [u'tipo_periodicidad_facturacion', u'Tipos de periodicidad de facturación'],  # 36 -
        [u'prioridad', u'Prioridades'],  # 37 -
        [u'formato_stock', u'Formato de los stocks'],  # 38 -
        [u'tipo_pregunta', u'Tipos de preguntas'],  # 39 -
        [u'estado_peticion_precios', u'Estados de las peticiones de precios'],  # 40 -
    ]

    l = len(tipos_registros)
    log(table_db, l)
    for x in range(l):
        n_divisa = x + 1

        tipos_registros[x][0] = tipos_registros[x][0].lower()
        registro, nombre = tipos_registros[x]

        if n_divisa % 100 == 0 or n_divisa == l:
            log(nombre, l, n_divisa)
        dc = {
            u'id': sanitize(n_divisa),
            u'registro': sanitize(registro),
            u'nombre': sanitize(nombre),
            u'empresa_id': empresa_id
        }
        data_base.dc_db[table_db][registro] = x + 1
        dict_to_sql(data_base, dc, table_db)


def insert_divisas(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'divisa', 'divisas')
    log(table_db, l)
    n_divisa = 0
    for idx in idxs:
        n_divisa += 1
        if n_divisa % 100 == 0 or n_divisa == l:
            log(idx, l, n_divisa)

        data_base.dc_db[table_erp][idx] = n_divisa
        nombre, valor, codigo_bc3 = file_rg[idx][:3]

        dc_ln = {
            u'id': sanitize(n_divisa),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'valor': sanitize(valor),
            u'codigo_bc3': sanitize(codigo_bc3),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_ln, table_db)


def insert_idiomas(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'idioma', 'idiomas')
    log(table_erp, l)
    n_idioma = 0
    for idx in idxs:
        n_idioma += 1
        if n_idioma % 100 == 0 or n_idioma == l:
            log(idx, l, n_idioma)

        data_base.dc_db[table_erp][idx] = n_idioma
        nombre = file_rg[idx][0]

        dc_ln = {
            u'id': sanitize(n_idioma),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_ln, table_db)


def insert_paises(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'pais', 'paises')
    log(table_erp, l)
    n_pais = 0
    for idx in idxs:
        n_pais += 1
        if n_pais % 100 == 0 or n_pais == l:
            log(idx, l, n_pais)

        data_base.dc_db[table_erp][idx] = n_pais
        nombre, iso, sepa, lon_iban, div, idi = file_rg[idx]

        dc_pais = {
            u'id': sanitize(n_pais),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_iso': sanitize(iso),
            u'norma_sepa': sanitize(sepa == 'S'),
            u'longitud_iban': sanitize(lon_iban),
            u'divisa_id': sanitize(data_base.dc_db['divisas'].get(div)),
            u'idioma_id': sanitize(data_base.dc_db['idiomas'].get(idi)),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_pais, table_db)


def insert_provincias(data_base):
    table_db = 'region'
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

    data_base.dc_db[table_db] = {}
    l = len(values_db)
    log(table_db, l)
    for x in range(l):
        nombre = values_db[x]
        n_region = x + 1
        if n_region % 10 == 0 or n_region == l:
            log(nombre, l, n_region)
        data_base.dc_db[table_db][nombre] = n_region
        dc_pais = {
            u'id': sanitize(n_region),
            u'nombre': sanitize(nombre),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_pais, table_db)

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

    table_db = 'provincia'
    data_base.dc_db[table_db] = {}
    l = len(values_db)
    log(table_db, l)
    for x in range(l):
        idx, nombre, codigo_mi, region_id = values_db[x]
        n_provincia = x + 1
        if n_provincia % 10 == 0 or n_provincia == l:
            log(nombre, l, n_provincia)
        data_base.dc_db[table_db][nombre] = n_provincia

        dc_pais = {
            u'id': sanitize(n_provincia),
            u'nombre': sanitize(nombre),
            u'codigo_mi': sanitize(codigo_mi),
            u'region_id': sanitize(region_id),
            u'pais_id': sanitize(data_base.dc_db['paises']['ESP']),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_pais, table_db)


def insert_clasificacionclientes(data_base):
    table_db, table_erp_cc, file_rg_cc, idxs_cc, l_cc, n_registro = get_data(data_base, 'clasificacioncliente',
                                                                             'categorias_clientes')
    table_db, table_erp_act, file_rg_act, idxs_act, l_act, n_registro = get_data(data_base, 'clasificacioncliente',
                                                                                 'actividades')
    table_db, table_erp_se, file_rg_se, idxs_se, l_se, n_registro = get_data(data_base, 'clasificacioncliente',
                                                                             'sectores')
    table_db, table_erp_ss, file_rg_ss, idxs_ss, l_ss, n_registro = get_data(data_base, 'clasificacioncliente',
                                                                             'subsectores')

    l = l_cc + l_act + l_se + l_ss
    log(table_db, l)
    n_clasificacion = 0
    # Actividades
    for idx in idxs_act:
        n_clasificacion += 1
        if n_clasificacion % 50 == 0 or n_clasificacion == l:
            log(sanitize(n_clasificacion), l, n_clasificacion)
        data_base.dc_db[table_erp_act][idx] = n_clasificacion

        nombre, cdant, precios, email = file_rg_act[idx]
        dc_act = {
            u'id': sanitize(n_clasificacion),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(data_base.dc_db['subtiporegistro']['actividad']),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_act, table_db)

    # Cat. clientes
    for idx in file_rg_cc.keys():
        n_clasificacion += 1
        if n_clasificacion % 100 == 0 or n_clasificacion == l:
            log(sanitize(n_clasificacion), l, n_clasificacion)
        data_base.dc_db[table_erp_cc][idx] = n_clasificacion

        nombre = file_rg_cc[idx][0]
        dc_cc = {
            u'id': sanitize(n_clasificacion),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(data_base.dc_db['subtiporegistro']['categoria_cliente']),
            u'codigo_anterior': sanitize(''),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_cc, table_db)

    # Sectores
    for idx in file_rg_se.keys():
        n_clasificacion += 1
        if n_clasificacion % 100 == 0 or n_clasificacion == l:
            log(sanitize(n_clasificacion), l, n_clasificacion)
        data_base.dc_db[table_erp_se][idx] = n_clasificacion

        nombre, cdant = file_rg_se[idx][:2]
        dc_se = {
            u'id': sanitize(n_clasificacion),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(data_base.dc_db['subtiporegistro']['sector']),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_se, table_db)

    # Sub sectores
    relacionados = list()
    for idx in idxs_ss:
        n_clasificacion += 1
        if n_clasificacion % 100 == 0 or n_clasificacion == l:
            log(sanitize(n_clasificacion), l, n_clasificacion)
        data_base.dc_db[table_erp_ss][idx] = n_clasificacion

        nombre, cdant, padre = file_rg_ss[idx][:3]
        dc_ss = {
            u'id': sanitize(n_clasificacion),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(data_base.dc_db['subtiporegistro']['sub_sector']),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_ss, table_db)
        id_padre = data_base.dc_db['sectores'][padre]
        i_relacion(data_base, [n_clasificacion], table_db, id_padre, n_registro, relacionados, data_base.dc_db)

    l = len(relacionados)
    log('Relaciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log('', l, i)
        data_base.query(sql)


def insert_caracteristica(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'caracteristica', 'nseries_caracteristica')
    log(table_db, l)
    n_car = 0
    for idx in idxs:
        n_car += 1
        if n_car % 100 == 0 or n_car == l:
            log(sanitize(idx), l, n_car)
        data_base.dc_db[table_erp][idx] = n_car

        nombre, tipo = file_rg[idx][:2]

        dc_car = {
            u'id': sanitize(n_car),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_valor': sanitize(tipo[0].upper()),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_car, table_db)


def insert_opciones_tipos(data_base):
    table_db = 'opciontipo'
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
        [u'P', u'Pendiente', 'estado_peticion_precios'],
        [u'R', u'Respondida', 'estado_peticion_precios'],
        [u'S', u'Servida', 'estado_peticion_precios'],
        [u'F', u'Finalizada', 'estado_peticion_precios'],
        [u'A', u'Anulada', 'estado_peticion_precios'],
    ]

    l = len(data)
    log(table_db, l)

    data_base.dc_db[table_db] = dict()
    n_opcion = 0
    for ln in data:
        n_opcion += 1
        codigo, nombre, id_ = ln

        if n_opcion % 100 == 0 or n_opcion == l:
            log(nombre, l, n_opcion)

        sub_tipo = data_base.dc_db['subtiporegistro'][id_]
        data_base.dc_db[table_db][codigo + '|' + id_] = n_opcion

        dc_opcion = {
            u'id': sanitize(n_opcion),
            u'tipo_id': sanitize(sub_tipo),
            u'nombre': sanitize(nombre),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_opcion, table_db)


def insert_fechas(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'fecha', 'nseries_fechas')
    log(table_db, l)

    n_fecha = 0
    for idx in idxs:
        n_fecha += 1
        if n_fecha % 100 == 0 or n_fecha == l:
            log(sanitize(idx), l, n_fecha)
        data_base.dc_db[table_erp][idx] = n_fecha

        nombre, per, tper, na = file_rg[idx]
        dc_fec = {
            u'id': sanitize(n_fecha),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'periodicidad': sanitize(per),
            u'tipo_periodicidad_id': sanitize(data_base.dc_db['opciontipo'].get(tper + '|tipo_periodicidad')),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_fec, table_db)


def insert_administradores(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'administrador', 'administradores')
    log(table_db, l)

    relacionados = list()

    n_administrador = 0
    for idx in idxs:
        n_administrador += 1
        if n_administrador % 100 == 0 or n_administrador == l:
            log(sanitize(idx), l, n_administrador)
        data_base.dc_db[table_erp][idx] = n_administrador

        nombre, direccion, poblacion, provincia, codigopostal, telefono, fax, dni, email, fechanacimiento, observaciones = \
        file_rg[idx][:11]
        if poblacion or codigopostal or direccion:
            i_direcciones(data_base, [[direccion, codigopostal, poblacion, provincia, 'ESP']], n_administrador,
                          n_registro, relacionados, True, True)

        if telefono or fax or email:
            i_contactos(data_base, [['', '', telefono, email, True, '', fax]], n_administrador, n_registro,
                        relacionados)

        observaciones = stripRtf(observaciones)

        dc_administrador = {
            u'id': sanitize(n_administrador),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(observaciones),
            u'observaciones': sanitize(observaciones),
            u'cif': sanitize(dni),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_administrador, table_db)

    log('Relacionados ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)


def insert_textos(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'texto', 'fcartas')
    log(table_db, l)

    n_nc = 0
    for idx in idxs:
        n_nc += 1
        if n_nc % 100 == 0 or n_nc == l:
            log(sanitize(idx), l, n_nc)
        data_base.dc_db[table_erp][idx] = n_nc

        nombre, txt = file_rg[idx][:2]
        dc_texto = {
            u'id': sanitize(n_nc),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'texto': sanitize(stripRtf(txt)),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_texto, table_db)


def insert_normativa(data_base):
    def _tabla_resumen(resumen, nck, tb_resu):
        j = 0
        for line in resumen:
            j += 1
            name, registro, campo, col, rel, ancho, filtro, valor = line

            registro = data_base.dc_db['tiporegistro'].get(registro)

            id_ = get_id(data_base, 'tablaresumen')
            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'texto_normativa_id': sanitize(nck),
                'nombre': sanitize(name),
                'registro': sanitize(registro),
                'campo': sanitize(campo),
                'ancho': sanitize(ancho),
                'filtro': sanitize(filtro),
                'valor': sanitize(valor),
                'orden': sanitize(j)
            }
            tb_resu.append(dc_ln)

    def _docs(lines, nck, docs):
        j = 0
        for line in lines:
            j += 1
            tipo_parte, idioma, oficial, documento, documento_2, tabla_resumen = line
            mar = False
            lis = list()
            tr_antes = ''
            tr_despues = ''
            if tabla_resumen:
                try:
                    if len(eval(tabla_resumen)) == 4:
                        lis, marc, tr_antes, tr_despues = eval(tabla_resumen)
                    else:
                        lis, marc, tr_antes = eval(tabla_resumen)

                except:
                    raise ValueError((eval(tabla_resumen)))

            id_ = get_id(data_base, 'textonormativa')

            idioma = data_base.dc_db['idiomas'].get(idioma)
            documento_2 = data_base.dc_db['fcartas'].get(documento_2)
            documento = data_base.dc_db['fcartas'].get(documento)
            tr_antes = stripRtf(tr_antes)
            tr_despues = stripRtf(tr_despues)
            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'checklist_id': sanitize(nck),
                'idioma_id': sanitize(idioma),
                'texto_antes_id': sanitize(documento),
                'texto_despues_id': sanitize(documento_2),
                'tipo_parte_id': tipo_parte,
                'horizontal': sanitize(mar),
                'texto_antes_resumen': sanitize(tr_antes),
                'texto_despues_resumen': sanitize(tr_despues),

            }
            docs.append(dc_ln)

            _tabla_resumen(lis, id_, docs)

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'normativacapitulo', 'normativa_capitulos')
    log(table_db, l)

    relacionados = list()

    n_normativacapitulo = 0
    for idx in idxs:
        n_normativacapitulo += 1
        if n_normativacapitulo % 100 == 0 or n_normativacapitulo == l:
            log(sanitize(idx), l, n_normativacapitulo)
        data_base.dc_db[table_erp][idx] = n_normativacapitulo

        nombre, idiomas = file_rg[idx]
        i_traducciones(data_base, idiomas, n_normativacapitulo, n_registro, relacionados)
        dc_normativacapitulo = {
            u'id': sanitize(n_normativacapitulo),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_normativacapitulo, table_db)

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'normativa')
    log(table_db, l)

    documents = list()
    n_nor = 0
    for idx in idxs:
        n_nor += 1
        if n_nor % 100 == 0 or n_nor == l:
            log(sanitize(idx), l, n_nor)
        data_base.dc_db[table_erp][idx] = n_nor

        nombre, imp, cert, apartado, subapartado, cap = file_rg[idx]
        capid = data_base.dc_db['normativa_capitulos'][cap]
        _docs(cert, n_nor, documents)
        dc_nor = {
            u'id': sanitize(n_nor),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'apartado': sanitize(apartado),
            u'sub_apartado': sanitize(subapartado),
            u'capitulo_normativa_id': sanitize(capid),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_nor, table_db)

    log('Relacionados ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)

    return documents


def insert_checklists(data_base):
    valores_predefinidos = list()
    relacionados = list()

    def _lineas(lines, ck, cks):
        j = 0
        for line in lines:
            j += 1
            num, padre_, descripcion, tipo_c, valorespredefinidos, obligatorio, valorpordefecto, periodicidad_c = line
            tipo_c = data_base.dc_db['opciontipo'][tipo_c + '|tipo_pregunta']
            periodicidad_c = data_base.dc_db['opciontipo'].get(periodicidad_c + '|tipo_periodicidad')

            id_ = get_id(data_base, 'checklistpregunta')

            if valorespredefinidos:
                valorespredefinidos = valorespredefinidos.split('|')
                for v in range(len(valorespredefinidos)):
                    id_v = get_id(data_base, 'valorpredefinido')
                    valores_predefinidos.append([id_v, id_, valorespredefinidos[v], v + 1])

            dc_ln = {
                'id': sanitize(id_),
                'empresa_id': '1',
                'checklists_id': sanitize(ck),
                'tipo_id': sanitize(tipo_c),
                'numero': sanitize(float(num)),
                'descripcion': sanitize(descripcion),
                'obligatorio': sanitize(obligatorio == 'S'),
                'tipo_periodicidad_id': sanitize(periodicidad_c),
                'valor_defecto': sanitize(valorpordefecto),
                'orden': sanitize(j),
            }
            cks.append(dc_ln)

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'checklist', 'checklists')
    log(table_db, l)
    n_chl = 0
    for idx in idxs:
        n_chl += 1
        if n_chl % 100 == 0 or n_chl == l:
            log(sanitize(idx), l, n_chl)
        data_base.dc_db[table_erp][idx] = n_chl
        nombre, lnas, norm, eti, tec_ls = file_rg[idx]
        _lineas(lnas, n_chl, relacionados)
        norm = data_base.dc_db['normativa'].get(norm)
        dc_ck = {
            u'id': sanitize(n_chl),
            u'codigo': sanitize(idx),
            u'normativa_id': sanitize(norm),
            u'nombre': sanitize(nombre),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_ck, table_db)

    table_rel = 'checklistpregunta'
    l = len(relacionados)
    log('Líneas ' + table_db, l)
    i = 0
    for dc in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log('', l, i)
        dict_to_sql(data_base, dc, table_rel)

    table_rel = 'valorpredefinido'
    l = len(valores_predefinidos)
    i = 0
    log('valores predefinidos ' + table_db, l)
    for value in valores_predefinidos:
        i += 1
        if i % 100 == 0 or i == l:
            log('', l, i)
        idh, padre, valor, orden = value
        dc_vp = {
            u'id': sanitize(idh),
            u'linea_checklist_id': sanitize(padre),
            u'valor': sanitize(valor),
            u'orden': sanitize(orden),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_vp, table_rel)


def insert_tipos_acciones(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'tipotrabajo', 'acciones_tipos')
    n_ta = 0
    for idx in idxs:
        n_ta += 1
        if n_ta % 100 == 0 or n_ta == l:
            log(sanitize(idx), l, n_ta)
        data_base.dc_db[table_erp][idx] = n_ta
        nombre, fecha, pri = file_rg[idx][:3]
        tipo_fecha_id = data_base.dc_db['nseries_fechas'].get(fecha)

        dc_ta = {
            u'id': sanitize(n_ta),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'prioridad': sanitize(pri),
            u'tipo_fecha_id': sanitize(tipo_fecha_id),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_ta, table_db)


def insert_clasificacionarticulos(data_base):
    table_db, table_erp_cat, file_rg_cat, idxs_cat, l_cat, n_registro = get_data(data_base, 'clasificacionarticulo',
                                                                                 'categorias')
    table_db, table_erp_gr, file_rg_gr, idxs_gr, l_gr, n_registro = get_data(data_base, 'clasificacionarticulo',
                                                                             'grupos')
    table_db, table_erp_fa, file_rg_fa, idxs_fa, l_fa, n_registro = get_data(data_base, 'clasificacionarticulo',
                                                                             'familias')
    table_db, table_erp_sf, file_rg_sf, idxs_sf, l_sf, n_registro = get_data(data_base, 'clasificacionarticulo',
                                                                             'subfamilias')

    ruta_rg_rgc = PATH + 'nseries_caracteristica'
    rgs_caracteristicas = bkopen(ruta_rg_rgc, huf=0)

    l = l_cat + l_gr + l_fa + l_sf
    log(table_db, l)

    relacionados = list()
    n_clasa = 0
    nr = data_base.dc_db['subtiporegistro']['categoria_articulo']
    for idx in idxs_cat:
        n_clasa += 1
        if n_clasa % 100 == 0 or n_clasa == l:
            log(sanitize(n_clasa), l, n_clasa)
        data_base.dc_db[table_erp_cat][idx] = n_clasa
        nombre = file_rg_cat[idx][0]
        dc_cat = {
            u'id': sanitize(n_clasa),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(nr),
            u'codigo_anterior': sanitize(''),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_cat, table_db)

    nr = data_base.dc_db['subtiporegistro']['grupo']
    for idx in idxs_gr:
        n_clasa += 1
        if n_clasa % 100 == 0 or n_clasa == l:
            log(sanitize(n_clasa), l, n_clasa)
        data_base.dc_db[table_erp_gr][idx] = n_clasa

        nombre, car, une, fechas, checks, cdant, idiomas = file_rg_gr[idx]

        i_caracteristicas(data_base, car, n_clasa, n_registro, relacionados, rgs_caracteristicas)
        i_fechas(data_base, fechas, n_clasa, n_registro, relacionados)
        i_traducciones(data_base, idiomas, n_clasa, n_registro, relacionados)
        i_checks(data_base, checks, n_clasa, n_registro, relacionados)

        dc_gr = {
            u'id': sanitize(n_clasa),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(nr),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_gr, table_db)

    relacionados = list()
    nr = data_base.dc_db['subtiporegistro']['familia']
    for idx in idxs_fa:
        n_clasa += 1
        if n_clasa % 100 == 0 or n_clasa == l:
            log(sanitize(n_clasa), l, n_clasa)
        data_base.dc_db[table_erp_fa][idx] = n_clasa

        nombre, padre, trsp, ctas, libre, car, fechas, checks, cdant, idiomas = file_rg_fa[idx]
        if padre not in data_base.dc_db['grupos'].keys():
            continue

        id_padre = data_base.dc_db['grupos'][padre]
        i_relacion(data_base, [n_clasa], table_db, id_padre, n_registro, relacionados)
        i_caracteristicas(data_base, car, n_clasa, n_registro, relacionados, rgs_caracteristicas)
        i_fechas(data_base, fechas, n_clasa, n_registro, relacionados)
        i_traducciones(data_base, idiomas, n_clasa, n_registro, relacionados)
        i_checks(data_base, checks, n_clasa, n_registro, relacionados)

        dc_fa = {
            u'id': sanitize(n_clasa),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(nr),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_fa, table_db)

    nr = data_base.dc_db['subtiporegistro']['sub_familia']
    for idx in idxs_sf:
        n_clasa += 1
        if n_clasa % 100 == 0 or n_clasa == l:
            log(sanitize(n_clasa), l, n_clasa)
        data_base.dc_db[table_erp_sf][idx] = n_clasa

        nombre, padre, une, car, fechas, checks, idiomas, cdant = file_rg_sf[idx]

        if padre not in data_base.dc_db['familias'].keys():
            continue

        id_padre = data_base.dc_db['familias'][padre]
        i_relacion(data_base, [n_clasa], table_db, id_padre, n_registro, relacionados)
        i_caracteristicas(data_base, car, n_clasa, n_registro, relacionados, rgs_caracteristicas)
        i_fechas(data_base, fechas, n_clasa, n_registro, relacionados,)
        i_traducciones(data_base, idiomas, n_clasa, n_registro, relacionados)
        i_checks(data_base, checks, n_clasa, n_registro, relacionados)

        dc_sf = {
            u'id': sanitize(n_clasa),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(nr),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_sf, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log('', l, i)
        data_base.query(sql)


def insert_zonas(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'zona', 'zonas')
    table_db, table_erp_sz, file_rg_sz, idxs_sz, l_sz, n_registro = get_data(data_base, 'zona', 'subzonas')

    log(table_db, l + l_sz)

    n_zona = 0
    for idx in idxs:
        n_zona += 1
        if n_zona % 100 == 0 or n_zona == l:
            log(sanitize(idx), l, n_zona)
        data_base.dc_db[table_erp][idx] = n_zona

        nombre, cdant = file_rg[idx]

        dc_zn = {
            u'id': sanitize(n_zona),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_zn, table_db)

    relacionados = list()
    for idx in idxs_sz:
        n_zona += 1
        if n_zona % 100 == 0 or n_zona == l:
            log(sanitize(idx), l, n_zona)
        data_base.dc_db[table_erp_sz][idx] = n_zona

        nombre, padre, cdant = file_rg_sz[idx]
        if not padre or padre not in data_base.dc_db['zonas'].keys():
            continue
        id_padre = data_base.dc_db['zonas'][padre]
        i_relacion(data_base, [n_zona], table_db, id_padre, n_registro, relacionados)

        dc_szn = {
            u'id': sanitize(n_zona),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_szn, table_db)

    l = len(relacionados)
    log('Relaciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log('', l, i)
        data_base.query(sql)


def insert_sociedades(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'sociedad', 'sociedades')
    log(table_db, l)

    n_sociedad = 0
    for idx in idxs:
        n_sociedad += 1
        if n_sociedad % 100 == 0 or n_sociedad == l:
            log(sanitize(idx), l, n_sociedad)
        data_base.dc_db[table_erp][idx] = n_sociedad

        nombre = file_rg[idx][0]
        dc_soc = {
            u'id': sanitize(n_sociedad),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(''),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_soc, table_db)


def insert_tarifas(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'tarifa', 'tarifas')
    log(table_db, l)
    n_tarifa = 0
    for idx in idxs:
        n_tarifa += 1
        if n_tarifa % 100 == 0 or n_tarifa == l:
            log(sanitize(idx), l, n_tarifa)
        data_base.dc_db[table_erp][idx] = n_tarifa

        nombre, mar, comi, cdant = file_rg[idx]
        dc_soc = {
            u'id': sanitize(n_tarifa),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(cdant),
            u'margen': sanitize(mar),
            u'comision': sanitize(comi),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_soc, table_db)


def insert_rutas(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'ruta', 'rutas')
    log(table_db, l)

    n_ruta = 0
    for idx in idxs:
        n_ruta += 1
        if n_ruta % 100 == 0 or n_ruta == l:
            log(sanitize(idx), l, n_ruta)
        data_base.dc_db[table_erp][idx] = n_ruta
        nombre, cdant, dias = file_rg[idx][:3]

        dc_soc = {
            u'id': sanitize(n_ruta),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_soc, table_db)


def insert_marcas(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'marca', 'marcas')
    log(table_db, l)

    n_marca = 0
    data_base.dc_db[table_erp] = dict()
    for idx in idxs:
        n_marca += 1
        if n_marca % 100 == 0 or n_marca == l:
            log(sanitize(idx), l, n_marca)

        data_base.dc_db[table_erp][idx] = n_marca

        nombre = file_rg[idx][0]
        dc_soc = {
            u'id': sanitize(n_marca),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(''),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_soc, table_db)


def insert_tipos(data_base):
    ls_t = [
        ['articulos_formatos', 16],
        ['contratos_ss', 17],
        ['costes_descompuestos', 18],
        ['delegaciones_tipos', 11],
        ['llamadas_tipos', 12],
        ['obras_tipos', 13],
        ['tipo_gastos', 15],
        ['vehiculos_tipos', 14]
    ]

    n_tipo = 0

    for ln_t in ls_t:
        table_erp, tipo = ln_t
        table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'tipo', table_erp)
        l = len(idxs)
        log(table_erp, l)

        x = 0
        for idx in idxs:
            n_tipo += 1
            x += 1
            if x % 100 == 0 or x == l:
                log(sanitize(idx), l, x)

            data_base.dc_db[table_erp][idx] = n_tipo
            nombre = file_rg[idx][0]
            dc_tipo = {
                u'id': sanitize(n_tipo),
                u'codigo': sanitize(idx),
                u'nombre': sanitize(nombre),
                u'codigo_anterior': sanitize(''),
                u'orden': sanitize(x),
                u'subtipo_id': sanitize(tipo),
                u'empresa_id': empresa_id
            }
            dict_to_sql(data_base, dc_tipo, table_db)

    return n_tipo


def insert_series(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'serie', 'series')
    log(table_db, l)

    n_serie = 0
    for idx in idxs:
        n_serie += 1
        if n_serie % 100 == 0 or n_serie == l:
            log(sanitize(idx), l, n_serie)

        data_base.dc_db[table_erp][idx] = n_serie

        nombre, doc, obs, post = file_rg[idx][:4]
        post_serie_ano = post == 'A'
        post_serie_mes = post == 'M'
        dc_serie = {
            u'id': sanitize(n_serie),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(''),
            u'post_serie_ano': sanitize(post_serie_ano),
            u'post_serie_mes': sanitize(post_serie_mes),
            u'observaciones': sanitize('\n'.join(obs)),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_serie, table_db)


def insert_tipos_contratos(n_tipo, data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'tipo', 'contratos_clases')
    log(table_erp, l)
    tipo = 10
    x = 0
    for idx in idxs:
        n_tipo += 1
        x += 1
        if x % 100 == 0 or x == l:
            log(sanitize(idx), l, x)
        data_base.dc_db[table_erp][idx] = n_tipo

        nombre, serp, cert, n1, serc = file_rg[idx][:5]
        cdant = ''

        dc_tipo = {
            u'id': sanitize(n_tipo),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(cdant),
            u'orden': sanitize(n_tipo),
            u'subtipo_id': sanitize(tipo),
            u'serie_certificados_id': sanitize(data_base.dc_db['series'].get(serc)),
            u'serie_partes_id': sanitize(data_base.dc_db['series'].get(serp)),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_tipo, table_db)


def insert_codigo_postal(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'codigopostal', 'postal')
    log(table_db, l)

    n_cp = 0
    for idx in idxs:
        pais, pob, prv, otro, ca = file_rg[idx]
        if len(idx) != 5:
            continue
        if idx[:2] not in data_base.dc_db['provincia'].keys():
            continue

        n_cp += 1
        if n_cp % 100 == 0 or n_cp == l:
            log(sanitize(idx), l, n_cp)
        data_base.dc_db[table_erp][idx] = n_cp
        pr = data_base.dc_db['provincia'][idx[:2]]

        dc_soc = {
            u'id': sanitize(n_cp),
            u'codigo': sanitize(idx),
            u'provincia_id': sanitize(pr),
            u'poblacion': sanitize(pob),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_soc, table_db)


def insert_almacenes(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'almacen', 'almacenes')
    log(table_db, l)

    relacionados = list()
    n_alm = 0
    for idx in idxs:
        n_alm += 1
        if n_alm % 100 == 0 or n_alm == l:
            log(sanitize(idx), l, n_alm)
        data_base.dc_db[table_erp][idx] = n_alm
        nombre, cm, vh, direc, cp, pob, pro, pais, tel, mail, cdant, tipo, blo = file_rg[idx]
        cdant = ''
        if direc or cp or pob:
            i_direcciones(data_base, [[direc, cp, pob, pro, pais]], n_alm, n_registro, relacionados,
                          True, True)

        if tel or mail:
            i_contactos(data_base, [['', '', tel, mail, True, '', '']], n_alm, n_registro, relacionados)

        code = tipo + '|tipo_almacen'
        id_tipo = data_base.dc_db['opciontipo'].get(code, data_base.dc_db['opciontipo']['IN|tipo_almacen'])
        dc_alm = {
            u'id': sanitize(n_alm),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'capacidad_maxima': sanitize(cm),
            u'codigo_anterior': sanitize(cdant),
            u'bloqueado': sanitize(blo == 'S'),
            u'tipo_id': sanitize(id_tipo),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_alm, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log(sanitize(''), l, i)
        data_base.query(sql)


def insert_bancos(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'banco', 'bancos')
    log(table_db, l)

    relacionados = list()
    n_ban = 0
    for idx in idxs:
        n_ban += 1
        if n_ban % 100 == 0 or n_ban == l:
            log(sanitize(idx), l, n_ban)
        data_base.dc_db[table_erp][idx] = n_ban
        (nombre, direc, pob, pro, cp, tel, fax, mail, contactos, tipo, n1, n2, n3, riesgo, ccta, obs, cdant,
         pref, n4, n5, iban, swift, suf, sep) = file_rg[idx]

        if direc or cp or pob or pro:
            lsd = [[direc, cp, pob, pro, '']]
            i_direcciones(data_base, lsd, n_ban, n_registro, relacionados, True, True)
        if tel or fax or mail:
            i_contactos(data_base, [['', '', tel, mail, True, '', fax]], n_ban, n_registro, relacionados)
        dc_ban = {
            u'id': sanitize(n_ban),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'is_caja': sanitize(tipo == 'C'),
            u'riesgo_total': sanitize(riesgo),
            u'iban': sanitize(iban),
            u'prefijos_csb': sanitize(pref),
            u'sufijo_csb': sanitize(suf),
            u'separador_csb': sanitize(sep),
            u'observaciones': sanitize('\n'.join(obs)),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_ban, table_db)

    l = len(relacionados)
    log('Direcciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log(sanitize(''), l, i)
        data_base.query(sql)


def insert_departamentos(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'departamento', 'departamentos')
    log(table_db, l)

    n_dep = 0
    for idx in idxs:
        n_dep += 1
        if n_dep % 100 == 0 or n_dep == l:
            log(sanitize(idx), l, n_dep)
        data_base.dc_db[table_erp][idx] = n_dep

        nombre = file_rg[idx][0]

        dc_szn = {
            u'id': sanitize(n_dep),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(''),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc_szn, table_db)


def insert_capitulos(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'capitulo', 'capitulos')
    log(table_db, l)

    relacionados = list()
    n_cap = 0
    for idx in idxs:
        n_cap += 1
        if n_cap % 100 == 0 or n_cap == l:
            log(sanitize(idx), l, n_cap)
        data_base.dc_db[table_erp][idx] = n_cap

        nombre, referencia, cdant, desc, idio = file_rg[idx]
        i_traducciones(data_base, idio, n_cap, n_registro, relacionados)

        dc = {
            u'id': sanitize(n_cap),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'codigo_anterior': sanitize(cdant),
            u'referencia': sanitize(referencia),
            u'descripcion_extendida': sanitize(desc),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log('', l, i)
        data_base.query(sql)


def insert_transportistas(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'transportista', 'transportistas')
    log(table_db, l)

    relacionados = list()
    n_tran = 0
    for idx in idxs:
        n_tran += 1
        if n_tran % 100 == 0 or n_tran == l:
            log(sanitize(idx), l, n_tran)
        data_base.dc_db[table_erp][idx] = n_tran

        nombre, cdant, aut, taut, nimo, cnae, direc, pob, cp, pro, pais, tel, email, cif = file_rg[idx][:14]

        if direc or cp or pob or pro:
            i_direcciones(data_base, [[direc, cp, pob, pro, pais]], n_tran, n_registro, relacionados, True, True)

        if tel or email or cif:
            i_contactos(data_base, [['', '', tel, email, True, '', '']], n_tran, n_registro, relacionados)

        dc = {
            u'id': sanitize(n_tran),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'cif': sanitize(cif),
            u'codigo_anterior': sanitize(cdant),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Direcciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log(sanitize(''), l, i)
        data_base.query(sql)


def insert_certificados(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'modelocertificado',
                                                                 'partes_certificado_modelo')
    log(table_db, l)

    n_mcert = 0
    for idx in idxs:
        n_mcert += 1
        if n_mcert % 100 == 0 or n_mcert == l:
            log(sanitize(idx), l, n_mcert)
        data_base.dc_db[table_erp][idx] = n_mcert
        nombre = file_rg[idx][0]

        dc = {
            u'id': sanitize(n_mcert),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc, table_db)

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'certificado', 'partes_certificado')

    log(table_db, l)

    n_cert = 0
    for idx in idxs:
        n_cert += 1
        if n_cert % 100 == 0 or n_cert == l:
            log(sanitize(idx), l, n_cert)
        data_base.dc_db[table_erp][idx] = n_cert
        nada, documento, secundario = file_rg[idx]

        dc = {
            u'id': sanitize(n_cert),
            u'codigo': sanitize(idx),
            u'secundario': sanitize(secundario == 'S'),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc, table_db)


def insert_formas_pago(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'modopago', 'fpago')
    log(table_db, l)

    n_fp = 0
    for idx in idxs:
        n_fp += 1
        if n_fp % 100 == 0 or n_fp == l:
            log(sanitize(idx), l, n_fp)
        data_base.dc_db[table_erp][idx] = n_fp
        (nombre, venc, cartera, tipo, caducidad_pago, dias, recargo, cdant, cuenta, avisar, banco, imp_iban, imp_cob,
         imp_deno) = file_rg[idx]

        tipo_id = data_base.dc_db['opciontipo'][tipo + '|tipo_cartera']
        banco_id = data_base.dc_db['bancos'].get(banco)
        dc = {
            u'id': sanitize(n_fp),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_id': sanitize(tipo_id),
            u'n_vencimientos': sanitize(venc),
            u'genera_cartera': sanitize(cartera == 'S'),
            u'caducidad_pago': sanitize(caducidad_pago),
            u'recargo_financiero': sanitize(recargo),
            u'numero_cuenta': sanitize(cuenta),
            u'aviso_cliente': sanitize(avisar == 'S'),
            u'imp_iban': sanitize(imp_iban == 'S'),
            u'imp_cobrado': sanitize(imp_cob == 'S'),
            u'imp_denominacion': sanitize(imp_deno == 'S'),
            u'codigo_anterior': sanitize(cdant),
            u'banco_id': sanitize(banco_id),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc, table_db)


def insert_personal(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'empleado', 'personal')
    log(table_db, l)

    relacionados = list()
    n_pers = 0
    for idx in idxs:
        n_pers += 1
        if n_pers % 100 == 0 or n_pers == l:
            log(sanitize(idx), l, n_pers)
        data_base.dc_db[table_erp][idx] = n_pers
        (nombre, email, domicilio, poblacion, provincia, codigo_postal, dni, observaciones, baja, fecha, motivo,
         contactos_ls, telefono_empresa, puesto, almacen_ls, numero_colegiado, rol_horario_laboral, contrasena,
         cdant, numero_seguridad_social, fecha_nacimiento, estado_civil, hijos, tipo_carnet_conducir, ccc,
         estado_actual, empresa_actual, antiguedad, fecha_alta_contrato_actual, tipo_contrato_actual,
         codigo_contrato_ss,
         obra_cliente_lugar, prorroga_contrato_actual, fin_del_contrato_actual, departamento, categoria,
         historico_categoria_puesto,
         email_empresa, coste_anual_empresa, protocolo_reconocimento_medico, fecha_ultimo_reconocimiento_medico,
         caducidad_reconocimiento_medico,
         restricciones_reconocimiento_medico, titulaciones_ls, homologaciones_ls, curso_prl_general,
         curso_prl_especifico, otros_cursos_ls,
         tarjeta_sanitaria_europea, es_vendedor, es_tecnico, es_cobrador, n1, n2,
         n3, permitir_firmar_certificados, albs_ls, facturas_ls, personal_externo, transportista, comision_general) = \
        file_rg[idx]

        if domicilio or codigo_postal or poblacion or provincia:
            i_direcciones(data_base, [[domicilio, codigo_postal, poblacion, provincia, '']], n_pers, n_registro,
                          relacionados, True, True)

        tipo_contrato_actual_id = None
        departamento_id = data_base.dc_db['departamentos'].get(departamento)
        almacen_id = None
        if almacen_ls:
            almacen_id = data_base.dc_db['transportistas'].get(almacen_ls[0])

        fecha = parser_date(fecha)
        fecha_nacimiento = parser_date(fecha_nacimiento)
        fecha_alta_contrato_actual = parser_date(fecha_alta_contrato_actual)
        prorroga_contrato_actual = parser_date(prorroga_contrato_actual)
        fin_del_contrato_actual = parser_date(fin_del_contrato_actual)
        fecha_ultimo_reconocimiento_medico = parser_date(fecha_ultimo_reconocimiento_medico)
        caducidad_reconocimiento_medico = parser_date(caducidad_reconocimiento_medico)
        antiguedad = parser_date(antiguedad)

        dc = {
            u'id': sanitize(n_pers),
            u'almacen_id': almacen_id,
            u'antiguedad': sanitize(antiguedad),
            u'baja': sanitize(baja == 'S'),
            u'caducidad_reconocimento': sanitize(caducidad_reconocimiento_medico),
            u'categoria': sanitize(categoria),
            u'cobrador': sanitize(es_cobrador == 'S'),
            u'codigo': sanitize(idx),
            u'codigo_anterior': sanitize(cdant),
            u'codigo_contrato_ss': sanitize(codigo_contrato_ss),
            u'comision_general': sanitize(comision_general),
            u'contrasena': sanitize(contrasena),
            u'coste_anual_empresa': sanitize(coste_anual_empresa),
            u'curso_prl_especifico': sanitize(curso_prl_especifico),
            u'curso_prl_general': sanitize(curso_prl_general),
            u'departamento_id': sanitize(departamento_id),
            u'dni': sanitize(dni),
            u'email': sanitize(email),
            u'email_empresa': sanitize(email_empresa),
            u'empresa_actual': sanitize(empresa_actual),
            u'estado_actual': sanitize(estado_actual),
            u'estado_civil': sanitize(estado_civil),
            u'fecha_alta_contrato': sanitize(fecha_alta_contrato_actual),
            u'fecha_baja': sanitize(fecha),
            u'fecha_nacimiento': sanitize(fecha_nacimiento),
            u'fecha_ultimo_reconocimento': sanitize(fecha_ultimo_reconocimiento_medico),
            u'fin_del_contrato_actual': sanitize(fin_del_contrato_actual),
            u'hijos': sanitize(hijos),
            u'historico_categoria': sanitize(historico_categoria_puesto),
            u'iban': sanitize(ccc),
            u'motivo_baja': sanitize(motivo),
            u'nombre': sanitize(nombre),
            u'numero_colegiado': sanitize(numero_colegiado),
            u'numero_ss': sanitize(numero_seguridad_social),
            u'obra_cliente_lugar': prorroga_contrato_actual,
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'permitir_firmar_cert': sanitize(permitir_firmar_certificados == 'S'),
            u'personal_externo': sanitize(personal_externo == 'S'),
            u'prorroga_contrato_actual': sanitize(prorroga_contrato_actual),
            u'protocolo_reconocimento': sanitize(protocolo_reconocimento_medico),
            u'puesto': sanitize(puesto),
            u'restricciones_reconocimento': sanitize(restricciones_reconocimiento_medico),
            u'tarjeta_sanitaria_eu': sanitize(tarjeta_sanitaria_europea),
            u'tecnico': sanitize(es_tecnico == 'S'),
            u'telefono_empresa': sanitize(telefono_empresa),
            u'telefono_personal': sanitize(''),
            u'tipo_carnet_conducir': sanitize(tipo_carnet_conducir),
            u'tipo_contrato_actual_id': tipo_contrato_actual_id,
            u'vendedor': sanitize(es_vendedor == 'S'),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Direcciones ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log(sanitize(''), l, i)
        data_base.query(sql)


def insert_tipos_partes(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'tipoordentrabajo', 'partes_tipos')
    log(table_db, l)

    relacionados = []
    n_tparte = 0
    for idx in idxs:
        n_tparte += 1
        if n_tparte % 100 == 0 or n_tparte == l:
            log(sanitize(idx), l, n_tparte)
        data_base.dc_db[table_erp][idx] = n_tparte
        nombre, cdar, taccion, cert, tec = file_rg[idx][:5]

        i_tecnicos(data_base, tec, n_tparte, n_registro, relacionados)

        taccion_id = data_base.dc_db['acciones_tipos'].get(taccion, 18)
        dc = {
            u'id': sanitize(n_tparte),
            u'codigo': sanitize(idx),
            u'nombre': sanitize(nombre),
            u'tipo_accion_id': sanitize(taccion_id),
            u'empresa_id': empresa_id
        }
        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql in relacionados:
        i += 1
        if i % 100 == 0 or i == l:
            log(sanitize(''), l, i)
        data_base.query(sql)


def insertar_documentos_normativa(ls, data_base):
    l = len(ls)
    log('Relacionados normativa', l)
    n_texto = 0
    omitidas = list()
    for dc in ls:
        n_texto += 1
        if n_texto % 100 == 0 or n_texto == l:
            log(str(dc['id']), l, n_texto)
        if 'tipo_parte_id' in dc.keys():
            table_rel = 'textonormativa'
            tp = dc['tipo_parte_id']
            tp = data_base.dc_db['partes_tipos'].get(tp)
            if tp is None:
                omitidas.append(dc['id'])
                continue
            dc['tipo_parte_id'] = sanitize(tp)
        else:
            table_rel = 'tablaresumen'
            if dc['texto_normativa_id'] in omitidas:
                continue
        dict_to_sql(data_base, dc, table_rel)


def insert_proveedores(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'proveedor', 'proveedores')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all(table_db)
        data_base.delete_where('direccion', 'registro=%d' % n_registro)
        data_base.delete_where('contacto', 'registro=%d' % n_registro)
        data_base.delete_where('ibanregistro', 'registro=%d' % n_registro)
        data_base.delete_where('relacionregistro', 'registro_padre=%d' % n_registro)

    n_proveedor = 0
    relacionados = list()
    for idx in idxs:
        if not idx.strip():
            continue
        n_proveedor += 1
        if n_proveedor % 100 == 0.:
            log(idx, len(idxs), n_proveedor)
        data_base.dc_db[table_erp][idx] = n_proveedor

        (nombre_fiscal, nombre_comercial, domicilio, poblacion, provincia, codigo_postal, telefono, fax, cif_dni, email,
         web, divisa,
         actividad_comercial, tipop, cuenta_cantable, cuenta_contable_efectos, bloqueado, observaciones, transportista,
         forma_pago,
         banco, sociedad, tipos_de_portes, tipo_facturacion_auto, descuento_general, descuento_pronto_pago, tipo_de_iva,
         pedir_a, cargos, fecha_ultima_compra, pais, cuenta_bases, tipo_compra, iban, bic_swift, primer_dia,
         segundo_dia, codigo_anterior,
         fecha_bloqueo, pedido_minimo_unidades, importe_pedido_minimo, motivo_bloqueo, contabilizado,
         integra_en_contabilidad,
         proveedor_de_facturacion, n2, n3, marca_serie, nima, cnae, autorizacion, tipo_de_autorizacion, zona, sub_zona,
         ruta,
         bloqueado_para_la_facturacion_automatica) = file_rg[idx]

        i_direcciones(data_base, [[domicilio, codigo_postal, poblacion, provincia, pais]], n_proveedor, n_registro,
                      relacionados, True, True)
        if telefono or fax or email:
            cargos.insert(0, ['Oficina', '', telefono, email, 'S', '', fax])
        i_contactos(data_base, cargos, n_proveedor, n_registro, relacionados)

        if iban:
            i_iban(data_base, [[iban, bic_swift, 'S']], n_proveedor, n_registro, relacionados)

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
        proveedor_facturacion = data_base.dc_db['rutas'].get(proveedor_de_facturacion)
        if proveedor_facturacion is not None:
            i_relacion(data_base, [n_proveedor], table_db, proveedor_facturacion, n_registro, relacionados)

        tipo_facturacion_auto = data_base.dc_db['opciontipo'].get(tipo_facturacion_auto + '|tipo_facturacion_auto')
        if tipo_facturacion_auto is None:
            tipo_facturacion_auto = data_base.dc_db['opciontipo'].get('T|tipo_facturacion_auto')

        tipo_de_iva = data_base.dc_db['opciontipo'].get(str(tipo_de_iva) + '|tipo_iva_proveedor')
        tipo = data_base.dc_db['opciontipo'].get(tipop + '|tipo_proveedor')
        marca_serie = None  # data_base.dc_db['marcasriesgo'].get(marca_serie)

        try:
            obs = stripRtf(observaciones)

        except KeyError as e:
            raise ValueError(type(observaciones))
            # obs = None

        dc = {
            u'id': sanitize(n_proveedor),
            u'empresa_id': sanitize(empresa_id),
            u'divisa_id': sanitize(divisa),
            u'actividad_id': sanitize(actividad),
            u'transportista_id': sanitize(transportista),
            u'metodo_pago_id': sanitize(forma_pago),
            u'banco_id': sanitize(banco),
            u'tipos_portes': sanitize(tipos_de_portes == 'P'),
            u'tipo_facturacion_id': sanitize(tipo_facturacion_auto),
            u'tipo_id': sanitize(tipo),
            u'sociedad_id': sanitize(sociedad),
            u'marca_serie_id': sanitize(marca_serie),
            u'zona_id': sanitize(zona),
            u'sub_zona_id': sanitize(sub_zona),
            u'ruta_id': sanitize(ruta),
            u'codigo': sanitize(idx),
            u'nombre_fiscal': sanitize(nombre_fiscal),
            u'nombre_comercial': sanitize(nombre_comercial),
            u'cif': sanitize(cif_dni),
            u'web': sanitize(web),
            u'cuenta_contable': sanitize(cuenta_cantable),
            u'cuenta_contable_efectos': sanitize(cuenta_contable_efectos),
            u'bloqueado': sanitize(bloqueado == 'S'),
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
            u'primer_dia_pago ': sanitize(primer_dia),
            u'segundo_dia_pago ': sanitize(segundo_dia),
            u'unidades_minimas': sanitize(pedido_minimo_unidades),
            u'importe_minimo': sanitize(importe_pedido_minimo),
            u'contabilizado': sanitize(contabilizado == 'S'),
            u'integra_contabilidad': sanitize(integra_en_contabilidad == 'S'),
            u'bloqueado_facturacion': sanitize(bloqueado_para_la_facturacion_automatica == 'S'),
            u'codigo_anterior': sanitize(codigo_anterior)

        }
        dict_to_sql(data_base, dc, table_db)

    log('Direcciones y contactos ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)


def insert_clientes(data_base):
    dc_delegaciones_defecto = {}
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'cliente', 'clientes')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all(table_db)
        data_base.delete_where('direccion', 'registro=%d' % n_registro)
        data_base.delete_where('contacto', 'registro=%d' % n_registro)
        data_base.delete_where('ibanregistro', 'registro=%d' % n_registro)
        data_base.delete_where('modelocertificadoregistro', 'registro=%d' % n_registro)
        data_base.delete_where('precioespecial', 'registro=%d' % n_registro)
        data_base.delete_where('vendedorregistro', 'registro=%d' % n_registro)
        data_base.delete_where('tecnicoregistro', 'registro=%d' % n_registro)

    n_cliente = 0
    relacionados = list()
    for idx in idxs:
        if not idx.strip():
            continue
        n_cliente += 1
        if n_cliente % 100 == 0.:
            log(idx, len(idxs), n_cliente)
        data_base.dc_db[table_erp][idx] = n_cliente

        (nombre_fiscal, nombre_comercial, cif_dni, codigo_anterior, direccion_, telefono_, sector_actividad,
         sub_sector_actividad,
         actividad, transportista, forma_de_pago, vendedor_por_defecto, cobrador, administrador_de_fincas,
         tarifa_precios,
         zona, moroso, delegacion_por_defecto, cuenta_contable, cuenta_contable_efectos, limite_credito, credito_actual,
         descuento_general, descuento_pronto_pago, comision_fija, fecha_creacion, fecha_ultima_venta, tipo_iva,
         tipo_impresion_factura, serie_para_documentos, n_copias_factura, primer_dia, segundo_dia, boqueado,
         agrupar_por_del_fac_contratos, facturar_directamente_contratos, cabecera_de_presupuestos,
         tipo_facturacion_automatica,
         valorar_albaranes, oficina_contable_factura_e, organo_gestor_factura_e, unidad_tramitadora_factura_e,
         tipo_factura_e,
         articulo_libre, ofertas_venta, iban_, familia_descuento, observaciones, observaciones_moroso, tipo_de_adeudo,
         numero_mandato, fecha_firma_mandato, marca_riesgo, riesgo_solicitado, fecha_solicitud_riesgo, riesgo_externo,
         fecha_de_bloqueo, motivo_del_bloqueo, sub_zona, receptor_mercancia_edi, comprobador_edi, departamento_edi,
         receptor_factura_edi, pagador_edi, contabilizado, no_integra_contabilidad, bloquear_en_facturacion_automatica,
         nada, periodicidad_facturacion, periodicidad_revisiones, tipo_periodicidad_revisiones, tecnico_s_por_defecto,
         traspasado_variable_interna, cabecera_facturas, categoria, latitud_variable_interna, longitud_variable_interna,
         cert_, divisa, sociedad, idioma_por_defecto, proveedor_vinculado, codigo_nima, codigo_cnae, nada, nada, ruta,
         periodicidad_envio_liquidaciones) = file_rg[idx]

        dc_delegaciones_defecto[idx] = delegacion_por_defecto

        for lnd in direccion_:
            i_direcciones(data_base, [[lnd[0], lnd[1], lnd[2], lnd[3], lnd[4]]], n_cliente, n_registro,
                          relacionados, True, True)
        for ln in telefono_:
            teleo, nom, cif, ema, fax, defe = ln[:6]
            i_contactos(data_base, [[nom, None, teleo, ema, defe, cif, fax]], n_cliente, n_registro, relacionados)
        i_iban(data_base, iban_, n_cliente, n_registro, relacionados)
        # i_relacion(data_base, vendedor_por_defecto, table_db, n_cliente, n_registro, relacionados, data_base.dc_db)
        i_modeloscertificado(data_base, cert_, n_cliente, n_registro, relacionados)
        i_precioespecial(data_base, ofertas_venta, n_cliente, n_registro, relacionados)
        i_vendedor(data_base, vendedor_por_defecto, n_cliente, n_registro, relacionados)
        if tecnico_s_por_defecto == ['']:
            tecnico_s_por_defecto = []
        i_tecnicos(data_base, tecnico_s_por_defecto, n_cliente, n_registro, relacionados)

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
        tarifa_precios = data_base.dc_db['tarifas'].get(tarifa_precios, data_base.dc_db['tarifas'].get('01'))
        idioma_por_defecto = data_base.dc_db['idiomas'].get(idioma_por_defecto)
        serie_para_documentos = data_base.dc_db['series'].get(serie_para_documentos)
        transportista = data_base.dc_db['transportistas'].get(transportista)

        tipo_iva = data_base.dc_db['opciontipo'].get(str(tipo_iva) + '|tipo_iva_cliente')
        tipo_facturacion_automatica = data_base.dc_db['opciontipo'].get(
            str(tipo_facturacion_automatica) + '|tipo_facturacion_auto')
        if tipo_facturacion_automatica is None:
            tipo_facturacion_automatica = data_base.dc_db['opciontipo'].get('T|tipo_facturacion_auto')
        marca_riesgo = None  # data_base.dc_db['marcasriesgo'].get(marca_serie)

        try:
            observaciones = stripRtf(observaciones)
        except TypeError as e:
            raise ValueError(observaciones)
            # observaciones = None

        dc = {
            'empresa_id': sanitize(empresa_id),
            'id': sanitize(n_cliente),
            'codigo': sanitize(idx),
            'factura_directa': sanitize(False),
            'actividad_id': sanitize(actividad),
            'administrador_id': sanitize(administrador_de_fincas),
            'agrupar_delegacion': sanitize(agrupar_por_del_fac_contratos),
            'bloquear_factu_auto': sanitize(bloquear_en_facturacion_automatica != 'S'),
            'boqueado': sanitize(boqueado == 'S'),
            'categoria_id': sanitize(categoria),
            'cif': sanitize(cif_dni),
            'codigo_anterior': sanitize(codigo_anterior),
            'comision': sanitize(comision_fija),
            'contabilizado': sanitize(contabilizado == 'S'),
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
            'valorar_albaranes': sanitize(valorar_albaranes == 'S'),
            'zona_id': sanitize(zona),

        }
        dict_to_sql(data_base, dc, table_db)

    log('Relacionados ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)

    return dc_delegaciones_defecto


def insert_delegaciones(delegaciones_defecto_ls, data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'delegacion', 'delegaciones')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all(table_db)
        data_base.delete_where('direccion', 'registro=%d' % n_registro)
        data_base.delete_where('contacto', 'registro=%d' % n_registro)
        data_base.delete_where('ibanregistro', 'registro=%d' % n_registro)
        data_base.delete_where('modelocertificadoregistro', 'registro=%d' % n_registro)
        data_base.delete_where('vendedorregistro', 'registro=%d' % n_registro)
        data_base.delete_where('precioespecial', 'registro=%d' % n_registro)
    data_base.dc_db[table_erp] = dict()

    n_cliente = 0
    relacionados = list()
    for idx in idxs:
        if not idx.strip():
            continue

        (cli, numerodedelegacion, nombrecomercial, codigoanterior, tipo, bloqueado, fechadebloqueo, motivobloqueo,
         tipoautorizaciondegestionderesiduos, direccionfacturacion_ls, direccion_ls, sectordeactividad,
         subsectordeactividad,
         actividadcomercial, formadepago, vendedor_ls, zona, observaciones, observacionestestasat, moroso, libre,
         iban_ls,
         subzona, receptormercancia_edi, comprobador_edi, departamento_edi, receptorfactura_edi, pagador_edi,
         traspasado,
         nointegraencontabilidad, _1dia, _2dia, cuentacontable, cuentacontableefectos, telefono_ls,
         administradordefincas,
         transportista, imagenespecialparaimpresiones, tecnicospordefecto, ofertas_venta_ls, tarifa, latiutud, longitud,
         cobrador, modelos_ls, divisa, sociedad, idiomapordefecto, autorizaciongestionresiduos, codigonima, codigocnae,
         ruta, autorizacionproductor, tipodeautorizacionproductor, nimaproductor, horario, categoria) = file_rg[idx]

        n_del = delegaciones_defecto_ls.get(cli)
        if n_del is None:
            continue

        cliente = data_base.dc_db['clientes'].get(cli)
        if cliente is None:
            continue

        n_cliente += 1
        if n_cliente % 100 == 0.:
            log(idx, len(idxs), n_cliente)
        data_base.dc_db[table_erp][idx] = n_cliente

        i_direcciones(data_base, direccionfacturacion_ls, n_cliente, n_registro, relacionados, True, False)
        i_direcciones(data_base, direccion_ls, n_cliente, n_registro, relacionados, False, True)

        for ln in telefono_ls:
            teleo, nom, cif, ema, fax, defe = ln[:6]
            i_contactos(data_base, [[nom, None, teleo, ema, defe, cif, fax]], n_cliente, n_registro, relacionados)
        i_iban(data_base, iban_ls, n_cliente, n_registro, relacionados)
        i_vendedor(data_base, vendedor_ls, n_cliente, n_registro, relacionados)
        i_modeloscertificado(data_base, modelos_ls, n_cliente, n_registro, relacionados)
        i_precioespecial(data_base, ofertas_venta_ls, n_cliente, n_registro, relacionados)

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
        except TypeError as e:
            raise ValueError(observacionestestasat)
            # obsi = None

        try:
            obs = stripRtf(observaciones)
        except TypeError as e:
            raise ValueError(observaciones)
            # obs = None

        dc = {
            u'empresa_id': sanitize(empresa_id),
            u'id': sanitize(n_cliente),
            u'codigo': sanitize(numerodedelegacion),
            u'segundo_dia_cobro': sanitize(_2dia),
            u'actividad_id': sanitize(actividadcomercial),
            u'administrador_id': sanitize(administradordefincas),
            u'bloqueado': sanitize(bloqueado == 'S'),
            u'categoria_id': sanitize(categoria),
            u'cliente_id': sanitize(cliente),
            u'codigo_anterior': sanitize(codigoanterior),
            u'divisa_id': sanitize(divisa),
            u'fecha_bloqueo': sanitize(parser_date(fechadebloqueo)),
            u'forma_pago_id': sanitize(formadepago),
            u'horario': sanitize(horario),
            u'idioma_id': sanitize(idiomapordefecto),
            u'latiutud': sanitize(latiutud),
            u'longitud': sanitize(longitud),
            u'motivo_bloqueo': sanitize(motivobloqueo),
            u'nombre_comercial': sanitize(nombrecomercial),
            u'observaciones': sanitize(obs),
            u'observaciones_internas': sanitize(obsi),
            u'primer_dia_cobro': sanitize(_1dia),
            u'ruta_id': sanitize(ruta),
            u'sector_id': sanitize(sectordeactividad),
            u'sociedad_id': sanitize(sociedad),
            u'sub_sector_id': sanitize(subsectordeactividad),
            u'sub_zona_id': sanitize(subzona),
            u'tarifa_id': sanitize(tarifa),
            u'tipo_id': sanitize(tipo),
            u'transportista_id': sanitize(transportista),
            u'traspasado': sanitize(traspasado == 'S'),
            u'zona_id': sanitize(zona),
            u'defecto': sanitize(numerodedelegacion == n_del),

        }
        dict_to_sql(data_base, dc, table_db)

    log('Relacionados ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 100 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)


def insert_articulos(data_base):
    def _proveedores(ls, na, rels):
        table_rel = 'articuloproveedor'
        for lnp in ls:
            proveedor_i, pcompra_ln, dto_ln, fec_u_compra_ln, alb_compra_id, referencia_ln, p_acordado_ln, defecto = lnp
            proveedor_id = data_base.dc_db['proveedores'].get(proveedor_i)
            if proveedor_id is None:
                continue
            id_p = get_id(data_base, table_rel) 
            
            dc_lnp = {
                'empresa_id': '1',
                'articulo_id': sanitize(na),
                'id': sanitize(id_p),
                'proveedor_id': sanitize(proveedor_id),
                'precio_compra': sanitize(pcompra_ln),
                'descuento': sanitize(dto_ln),
                'fecha_ultima_compra': sanitize(parser_date(fec_u_compra_ln)),
                # 'albaran_compra': sanitize(alb_compra_id), #TODO
                'referencia': sanitize(referencia_ln),
                'precio_acordado': sanitize(p_acordado_ln),
                'defecto': sanitize(defecto == 'S'),
            }
            rels.append(dict_to_sql(data_base, dc_lnp, table_rel, False))

    def _descompuestos(ls, nd, rels):
        for lnd in ls:
            cdar_id, deno_ln, uds_ln, pcoste_ln = lnd
            id_d = get_id(data_base, 'id_art_desc')
            
            dc_lnd = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'articulo_padre_id': sanitize(nd),
                'articulo_id': sanitize(cdar_id),
                'nombre': sanitize(deno_ln),
                'unidades': sanitize(uds_ln),
                'precio_coste': sanitize(pcoste_ln),
            }
            rels.append(dc_lnd)

    def _stock(lst, ns, rels):
        table_rel = 'stock'
        for lnd in lst:
            calm, ubi, stock_a, stock_imputado, minimo, maximo, pedir_de = lnd
            alm = data_base.dc_db['almacenes'].get(calm)
            if alm is None:
                continue
            id_d = get_id(data_base, 'id_stock')
            dc_lns = {
                'id': sanitize(id_d),
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
            rels.append(dict_to_sql(data_base, dc_lns, table_rel, False))

    def _tarifa(lsta, nt, rels):
        table_rel = 'articulotarifa'
        for lnd in lsta:
            tar, margen_, precio_venta, comision_ = lnd
            tarifa = data_base.dc_db['tarifas'].get(tar)
            if tarifa is None:
                print(tar)
                continue

            id_d = get_id(data_base, table_rel)

            dc_lnt = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'tarifa_id': sanitize(tarifa),
                'articulo_id': sanitize(nt),
                'margen': sanitize(margen_),
                'precio_venta': sanitize(precio_venta),
                'comision': sanitize(comision_),
            }
            rels.append(dict_to_sql(data_base, dc_lnt, table_rel, False))

    def _facturar(lsta, nt, rels):
        for lnd in lsta:
            taccion, artfac = lnd
            tipoaccion = data_base.dc_db['acciones_tipos'].get(taccion)
            if tipoaccion is None:
                continue

            id_d = get_id(data_base, 'articulofacturar')

            dc_lnf = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'tipo_accion_id': sanitize(tipoaccion),
                'articulo_id': sanitize(nt),
                'articulo_facturar_id': artfac,
            }
            rels.append(dc_lnf)

    def _comisionvendedor(lsta, nt, rels):
        table_rel = 'articulocomisionvendedor'
        for lnd in lsta:
            vendedor, comision_1, comision_2, porcentaje_minimo, porcentaje_maximo = lnd
            vendedor = data_base.dc_db['personal'].get(vendedor)

            id_d = get_id(data_base, table_rel)

            dc_lnc = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'articulo_id': sanitize(nt),
                'vendedor_id': sanitize(vendedor),
                'comision_1': sanitize(comision_1),
                'comision_2': sanitize(comision_2),
                'porcentaje_minimo': sanitize(porcentaje_minimo),
                'porcentaje_maximo': sanitize(porcentaje_maximo),
            }
            rels.append(dict_to_sql(data_base, dc_lnc, table_rel, False))

    def _ofertasventa(lsta, nt, rels):
        table_rel = 'ofertaventa'
        for lnd in lsta:
            desdefecha, hastafecha, precio, descuento, tarifa, unidades_minimas, tipocliente = lnd
            tarifa = data_base.dc_db['tarifas'].get(tarifa)

            id_d = get_id(data_base, table_rel)

            dc_lnc = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'articulo_id': sanitize(nt),
                'tarifa_id': sanitize(tarifa),
                'desde_fecha': sanitize(parser_date(desdefecha)),
                'hasta_fecha': sanitize(parser_date(hastafecha)),
                'precio': sanitize(precio),
                'descuento': sanitize(descuento),
                'unidades_minimas': sanitize(unidades_minimas),
            }
            rels.append(dict_to_sql(data_base, dc_lnc, table_rel, False))

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'articulo', 'articulos')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('articulocompuestoplantilla')
        data_base.delete_all('articuloproveedor')
        data_base.delete_all('articulofacturar')
        data_base.delete_all('articulotarifa')
        data_base.delete_all('articulocomisionvendedor')
        data_base.delete_all('stock')
        data_base.delete_all('ofertaventa')
        data_base.delete_all(table_db)
        data_base.delete_where('traduccion', 'registro=%d' % n_registro)
        data_base.delete_where('checklistregistro', 'registro=%d' % n_registro)
        data_base.delete_where('caracteristicaregistro', 'registro=%d' % n_registro)
        data_base.delete_where('fecharegistro', 'registro=%d' % n_registro)
        data_base.delete_where('modelocertificadoregistro', 'registro=%d' % n_registro)
        data_base.delete_where('checklistsecundarioregistro', 'registro=%d' % n_registro)

    ruta_c = PATH + 'nseries_caracteristicas'
    rgs_caracteristicas = bkopen(ruta_c, huf=0)

    n = 0
    relacionados = list()
    facturar_ls = list()
    descompuestos = list()

    for idx in idxs:
        if not idx.strip():
            continue
        n += 1
        if n % 1000 == 0.:
            log(idx, len(idxs), n)
        (nombre_ar, ean, codigo_anterior, grupo, familia, sub_familia, nada, nada, tipo_articulo, tipo_iva,
         descatalogado, fecha_descatalogado, motivo_descatalogado, estocable, precio_ultima_compra, precio_medio_coste,
         precio_coste, margen, comision, precio_base_de_venta, descripcion_extendida, visible_en_cocosat,
         obligatorio_n_serie_en_ventas,
         formato_stock, periodicidad_ls, checklist_ls, tarifa_ls, articulo_desc_ls, arpop_ls, vendedor_comisiones_ls,
         stock_ls, ofertas_venta_ls, almacen, fecha_u_compra, fecha_u_venta, fecha_u_inventario, cuenta_contable_ventas,
         cuenta_contable_compras, fecha_ul_fabricacion, precio_u_fabricacion, nada, unidades_escandallo,
         lote_minimo_de_produccion,
         gestion_de_lotes, categoria, articulo_generico, gestion_de_descompuesto, gestion_de_numeros_de_serie,
         desc_ext_idioma_ls,
         cargar_u_checklst, dias_caducidad_lotes, visible_en_testastore, fecha_de_validez_precio_coste, nada, nada,
         desagrupar_checklists, nada, nada, excluir_de_las_impresiones, desagrupar_al_importar_en_descompuestos,
         nada, nada, nada, fases, horas_facturadas, nada, nada, tipo_fecha_ls, caracteristicas_ls, articulo_facturar_ls,
         obligatorio_numero_de_serie_en_compras, idioma_ls, certs_ls, nada, agrupar_unidades_a_pedir) = file_rg[idx]
        if descripcion_extendida:
            descripcion_extendida = stripRtf(descripcion_extendida)

        formato_stock = formato_stock.upper()
        if formato_stock == 'U':
            formato_stock = 'UD'
        data_base.dc_db[table_erp][idx] = n

        categoria = None
        grupo = data_base.dc_db['grupos'].get(grupo)
        familia = data_base.dc_db['familias'].get(familia)
        sub_familia = data_base.dc_db['subfamilias'].get(sub_familia)
        tipo_articulo = data_base.dc_db['opciontipo'].get(tipo_articulo + '|tipo_articulo')
        formato_stock = data_base.dc_db['opciontipo'].get(formato_stock + '|formato_stock')
        almacen = data_base.dc_db['almacenes'].get(almacen)
        _proveedores(arpop_ls, n, relacionados)
        _descompuestos(articulo_desc_ls, n, descompuestos)
        _comisionvendedor(vendedor_comisiones_ls, n, relacionados)
        _ofertasventa(ofertas_venta_ls, n, relacionados)

        i_traducciones(data_base, idioma_ls, n, n_registro, relacionados)
        i_fechas(data_base, tipo_fecha_ls, n, n_registro, relacionados)
        i_caracteristicas(data_base, caracteristicas_ls, n, n_registro, relacionados, rgs_caracteristicas)
        i_checks(data_base, checklist_ls, n, n_registro, relacionados)
        _stock(stock_ls, n, relacionados)
        _tarifa(tarifa_ls, n, relacionados)
        _facturar(articulo_facturar_ls, n, facturar_ls)
        i_checksecundario(data_base, certs_ls, n, n_registro, relacionados)

        dc = {
            u'actu_precio_proveedor': sanitize(False),
            u'desagrupar_pprecios': sanitize(agrupar_unidades_a_pedir == 'S'),
            u'almacen_defecto_id': sanitize(almacen),
            u'cargar_ultimo_checklist': sanitize(cargar_u_checklst == 'S'),
            u'categoria_id': sanitize(categoria),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'comision': sanitize(comision),
            u'cuenta_contable_c': sanitize(cuenta_contable_compras),
            u'cuenta_contable_v': sanitize(cuenta_contable_ventas),
            u'desagrupar_checklist': sanitize(desagrupar_checklists == 'S'),
            u'bloqueado': sanitize(descatalogado == 'S'),
            u'desc_extendida': sanitize(descripcion_extendida),
            u'dias_caducidad_lotes': sanitize(dias_caducidad_lotes),
            u'ean': sanitize(ean),
            u'estocable': sanitize(estocable == 'S'),
            u'excluir_impresiones': sanitize(excluir_de_las_impresiones == 'S'),
            u'familia_id': sanitize(familia),
            u'fecha_validez_pcoste': sanitize(Num_aFecha(fecha_de_validez_precio_coste)),
            u'fecha_bloqueo': sanitize(Num_aFecha(fecha_descatalogado)),
            u'fecha_u_compra': sanitize(Num_aFecha(fecha_u_compra)),
            u'fecha_u_inventario': sanitize(Num_aFecha(fecha_u_inventario)),
            u'fecha_u_venta': sanitize(Num_aFecha(fecha_u_venta)),
            u'formato_stock_id': sanitize(formato_stock),
            u'ges_descompuesto': sanitize(gestion_de_descompuesto == 'S'),
            u'ges_lotes': sanitize(gestion_de_lotes == 'S'),
            u'ges_n_serie': sanitize(gestion_de_numeros_de_serie == 'S'),
            u'grupo_id': sanitize(grupo),
            u'horas_facturadas': sanitize(horas_facturadas),
            u'margen': sanitize(margen),
            u'motivo_bloqueo': sanitize(motivo_descatalogado),
            u'nombre': sanitize(nombre_ar),
            u'obligatorio_n_serie': sanitize(obligatorio_n_serie_en_ventas == 'S'),
            u'precio_base_venta': sanitize(precio_base_de_venta),
            u'precio_coste': sanitize(precio_coste),
            u'precio_medio_coste': sanitize(precio_medio_coste),
            u'precio_ultima_compra': sanitize(precio_ultima_compra),
            u'sub_familia_id': sanitize(sub_familia),
            u'tipo_articulo_id': sanitize(tipo_articulo),
            u'tipo_iva': sanitize(tipo_iva),
            u'visible_app': sanitize(visible_en_cocosat == 'S'),
            u'empresa_id': empresa_id,
            u'codigo': sanitize(idx),
            u'id': sanitize(n),

        }
        dict_to_sql(data_base, dc, table_db)

    log('Relacionados ' + table_db, len(relacionados))
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 1000 == 0.:
            log('', len(relacionados), i)
        data_base.query(sql_r)

    table_db = 'articulofacturar'
    log('Artículos facturación ', len(facturar_ls))
    i = 0
    for dc_ln in facturar_ls:
        i += 1
        if i % 10 == 0.:
            log('', len(facturar_ls), i)

        dc_ln['articulo_facturar_id'] = sanitize(data_base.dc_db['articulos'].get(dc_ln['articulo_facturar_id']))
        if dc_ln['articulo_facturar_id'] is None:
            continue
        dict_to_sql(data_base, dc_ln, table_db)

    total = 0
    table_db = 'articulocompuestoplantilla'
    log('Plantillas artículos compuestos', len(descompuestos))
    for dc_ln in descompuestos:
        cdar = data_base.dc_db['articulos'].get(dc_ln['articulo_id'])
        total += 1
        if total % 100 == 0.:
            log(cdar, total, len(descompuestos))
        if cdar is None:
            continue
        dc_ln['articulo_id'] = sanitize(cdar)

        dict_to_sql(data_base, dc_ln, table_db)


def insert_descompuestos(data_base):
    def _costes(ls, na, rels):
        table_rel = 'articulocompuestocosteindirecto'
        for lnp in ls:
            tipo_coste, porcentaje = lnp
            tipo_coste = data_base.dc_db['costes_descompuestos'].get(tipo_coste)
            if tipo_coste is None:
                continue

            dc_lnp = {
                'empresa_id': '1',
                'id': sanitize(na),
                'tipo_coste_id': sanitize(tipo_coste),
                'porcentaje': sanitize(porcentaje),
            }
            rels.append(dict_to_sql(data_base, dc_lnp, table_rel, False))

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'articulocompuesto', 'descompuestos')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('articulocompuesto')
        data_base.delete_all('articulocompuestocosteindirecto')
        # data_base.delete_where('checklistsecundarioregistros', 'registro=%d' % n_registro)

    lineas_ls = list()
    relacionados = list()

    n = 0
    c = 0
    for idx in idxs:
        if not idx.strip():
            continue

        (cdar, nombre, unidades_padre, actualiza_precio_de_venta, actualiza_precio_de_coste, codigo_documento_padre,
         archivo_documento_padre, descompuesto_original, lineas, costes_ls, observaciones) = file_rg[idx]

        idx = sanitize(idx)

        id_registro = 1  # codigo_documento_padre TODO
        registro = 1  # data_base.dc_db['tiporegistros'].get(archivo_documento_padre) TODO
        articulo = data_base.dc_db['articulos'].get(cdar)
        if articulo is None:
            c += 1
            continue

        n += 1
        if n % 500 == 0.:
            log(idx, len(idxs), n)
        data_base.dc_db[table_erp][idx] = n#TODO
        '''''¡: sanitize(lineas), 
            '': sanitize(costes_ls),'''
        _costes(costes_ls, n, relacionados)

        dc = {
            'id': sanitize(n),
            'codigo': sanitize(idx),
            'empresa_id': sanitize(1),
            'articulo_padre_id': sanitize(articulo),
            'nombre': sanitize(nombre),
            'unidades_padre': sanitize(unidades_padre),
            'actu_pventa': sanitize(actualiza_precio_de_venta == 'S'),
            'actu_pcoste': sanitize(actualiza_precio_de_coste == 'S'),
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


def insert_pprecios(data_base):
    def _lineas(lsta, nt, rels):
        table_rel = 'peticionprecioslinea'
        orden = 0
        for lnd in lsta:
            orden += 1
            (cdar, nombre, referencia, unidades, precio_coste, unidades_disponibles, precio_proveedor, fecha_servir,
             unidades_pedidas, coste, estado_, diccionario, registro, info, observaciones_, idlinea) = lnd
            cdar = data_base.dc_db['articulos'].get(cdar)
            if cdar is None:
                continue

            id_d = get_id(data_base, table_rel)

            estado_ = data_base.dc_db['opciontipo'].get(
                estado_ + "|estado_peticion_precios")  # "P|estado_peticion_precios"
            dcln = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'peticion_precios_id': sanitize(nt),
                'articulo_id': sanitize(cdar),
                'nombre': sanitize(nombre),
                'referencia': sanitize(referencia),
                'unidades': sanitize(unidades),
                'precio_coste': sanitize(precio_coste),
                'unidades_disponibles': sanitize(unidades_disponibles),
                'precio_proveedor': sanitize(precio_proveedor),
                'fecha_servir': sanitize(parser_date(fecha_servir)),
                'unidades_pedidas': sanitize(unidades_pedidas),
                'coste': sanitize(coste),
                'estado_id': sanitize(estado_),
                'observaciones': sanitize(stripRtf(observaciones_)),
                'orden': sanitize(orden),
            }
            rels.append(dict_to_sql(data_base, dcln, table_rel, False))

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'peticionprecios', 'peticion_precios')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('peticionprecios')
        data_base.delete_all('peticionprecioslinea')

    relacionados = list()

    n = 0
    c = 0
    for idx in idxs:
        if not idx.strip():
            continue

        n += 1
        if n % 25 == 0. or n == l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n

        (descripcion_peticion, fecha, proveedor_, estado, pedidos_cliente_ls, presupuestos_ls, obra_ls, lineas, hist,
         serie, pedido_proveedor_ls, observaciones, pedidos_almacen, coste_transporte, fecha_validez) = file_rg[idx]

        proveedor = data_base.dc_db['proveedores'].get(proveedor_)
        if proveedor is None:
            raise ValueError(proveedor_, idx)
        estado = data_base.dc_db['opciontipo'].get(estado + "|estado_peticion_precios")  # "P|estado_peticion_precios"
        serie = data_base.dc_db['series'].get(serie)

        _lineas(lineas, n, relacionados)

        dc = {
            u'id': sanitize(n),
            u'empresa_id': empresa_id,
            u'codigo': sanitize(idx),
            u'proveedor_id': sanitize(proveedor),
            u'estado_id': sanitize(estado),
            u'serie_id': sanitize(serie),
            u'descripcion': sanitize(descripcion_peticion),
            u'fecha': sanitize(parser_date(fecha)),
            u'fecha_validez': sanitize(parser_date(fecha_validez)),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'coste_transporte': sanitize(coste_transporte),

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 25 == 0. or i == l:
            log('', l, i)
        data_base.query(sql_r)


def insert_pedidoproveedor(data_base):
    def _lineas(lsta, nt, rels):
        table_rel = 'pedidoproveedorlinea'
        orden = 0
        for lnd in lsta:
            orden += 1
            (cdar, deno, refe, uds, urs_se, precio, preciodivisa, descuento, tiva, fechaservir, estadoln,
             desc_ext, idlnpcl, info, linea, idlnpa) = lnd
            cdar = data_base.dc_db['articulos'].get(cdar)
            if cdar is None:
                continue

            id_d = get_id(data_base, table_rel)

            estado_ = data_base.dc_db['opciontipo'].get(estadoln + "|estado_pedido")  # "P|estado_peticion_precios"
            dcln = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'pedido_proveedor_id': sanitize(nt),
                'articulo_id': sanitize(cdar),
                'nombre': sanitize(deno),
                'referencia': sanitize(referencia),
                'unidades': sanitize(uds),
                'precio': sanitize(precio),
                'descuento_1': sanitize(descuento),
                'descuento_2': sanitize(0.),
                'descuento_3': sanitize(0.),
                'descuento_4': sanitize(0.),
                'tipo_iva': sanitize(tiva),
                'estado_id': sanitize(estado_),
                'descripcion_extendida': sanitize(stripRtf(desc_ext)),
                'orden': sanitize(orden),
            }
            rels.append(dict_to_sql(data_base, dcln, table_rel, False))

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'pedidoproveedor', 'pedidos_pr')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('pedidoproveedor')
        data_base.delete_all('pedidoproveedorlinea')
        data_base.delete_where('relacionregistro', 'registro_padre=%d' % n_registro)
        data_base.delete_where('direccion', 'registro=%d' % n_registro)
        data_base.delete_where('ivaregistro', 'registro=%d' % n_registro)
        data_base.delete_where('baseimponible', 'registro=%d' % n_registro)

    relacionados = list()
    data_base.dc_db[table_db] = {}
    n = 0
    for idx in idxs:
        if not idx.strip():
            continue

        n += 1
        if n % 25 == 0. or n == l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n
        data_base.dc_db[table_db][idx] = n

        (proveedor_, almacen, fecha_pedido, fecha_servir, referencia, pp_lna, descuento_general, descuento_pronto_pag,
         forma_de_pago, observaciones, pp_bas, total, total_descuento_gene, total_descuento_pron, neto, impuestos,
         total, transportista, estado, pedido_por, gastos_financieros, gastos_financieros, coste_transporte,
         peso_aproximado, iva_fijo_documento, tipo_de_iva_del_prov, pp_iva, pp_pdc, serie, peticion_de_precios,
         obra, pedido_almacen, divisa, valor_divisa, sociedad, pp_basd, bruto_divisa, total_descuento_gene,
         total_descuento_pron, neto_divisa, impuestos_divisa, total_divisa, gastos_financieros_d,
         coste_transporte, pp_direc, tipo_de_portes, referencia, parte) = file_rg[idx]

        proveedor = data_base.dc_db['proveedores'].get(proveedor_)
        if proveedor is None:
            raise ValueError(proveedor_, idx)
        estado = data_base.dc_db['opciontipo'].get(estado + "|estado_peticion_precios")  # "P|estado_peticion_precios"
        tipo_de_iva_del_prov = data_base.dc_db['opciontipo'].get(
            "%d|tipo_iva_proveedor" % tipo_de_iva_del_prov)  # "P|estado_peticion_precios"
        serie = data_base.dc_db['series'].get(serie)
        transportista = data_base.dc_db['transportistas'].get(transportista)
        almacen = data_base.dc_db['almacenes'].get(almacen)
        divisa = data_base.dc_db['divisas'].get(divisa)
        sociedad = data_base.dc_db['sociedades'].get(sociedad)
        forma_de_pago = data_base.dc_db['fpago'].get(forma_de_pago)
        i_relacion(data_base, [peticion_de_precios], 'peticionprecios', n, n_registro, relacionados, 'peticion_precios')
        if parte:
            data_base.dc_db['relaciones_faltan'].append(['relacionregistro', n, n_registro, 'partes', parte])
        if obra:
            data_base.dc_db['relaciones_faltan'].append(['relacionregistro', n, n_registro, 'obras', obra])
        if pedido_almacen:
            data_base.dc_db['relaciones_faltan'].append(
                ['relacionregistro', n, n_registro, 'pedidos_am', pedido_almacen])

        _lineas(pp_lna, n, relacionados)
        if iva_fijo_documento:
            iva_fijo_documento = int(iva_fijo_documento)
        else:
            iva_fijo_documento = None
        dc = {
            u'id': sanitize(n),
            u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'almacen_id': sanitize(almacen),
            u'coste_transporte': sanitize(coste_transporte),
            u'descuento_general': sanitize(descuento_general),
            u'descuento_ppago': sanitize(descuento_pronto_pag),
            u'divisa_id': sanitize(divisa),
            u'estado_id': sanitize(estado),
            u'fecha_pedido': sanitize(parser_date(fecha_pedido)),
            u'fecha_servir': sanitize(parser_date(fecha_servir)),
            u'modo_pago_id': sanitize(forma_de_pago),
            u'gastos_financieros': sanitize(gastos_financieros),
            u'impuestos': sanitize(impuestos),
            u'iva_fijo': sanitize(iva_fijo_documento),
            u'neto': sanitize(neto),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'pedido_por': sanitize(pedido_por),
            u'peso_aproximado': sanitize(peso_aproximado),
            # u'peticion_precios': sanitize(peticion_de_precios),
            u'proveedor_id': sanitize(proveedor),
            u'referencia': sanitize(referencia),
            u'serie_id': sanitize(serie),
            u'sociedad_id': sanitize(sociedad),
            u'tipo_iva_proveedor_id': sanitize(tipo_de_iva_del_prov),
            u'portes_pagados': sanitize(tipo_de_portes == 'P'),
            u'total': sanitize(total),
            u'transportista_id': sanitize(transportista),
            u'valor_divisa': sanitize(valor_divisa)

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 25 == 0. or i == l:
            log('', l, i)
        data_base.query(sql_r)


def insert_alb_compra(data_base):
    def _lineas(lsta, nt, rels):
        table_rel = 'albarancompralinea'
        orden = 0
        for lnd in lsta:
            orden += 1
            (cdar, deno, referencia, unidades, precio_coste, precioostedivisa, descuento, iva, almacen, idlnp, nserie,
             info, descripcion_extendida, idl, di, libre) = lnd
            cdar = data_base.dc_db['articulos'].get(cdar)
            almacen = data_base.dc_db['almacenes'].get(almacen)
            if cdar is None:
                continue

            id_d = get_id(data_base, table_rel)

            tiva = int(iva)

            dcln = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'albaran_id': sanitize(nt),
                'articulo_id': sanitize(cdar),
                'almacen_id': sanitize(almacen),
                'nombre': sanitize(deno),
                'referencia': sanitize(referencia),
                'unidades': sanitize(unidades),
                'precio': sanitize(precio_coste),
                'descuento_1': sanitize(descuento),
                'descuento_2': sanitize(0.),
                'descuento_3': sanitize(0.),
                'descuento_4': sanitize(0.),
                'tipo_iva': sanitize(tiva),
                'descripcion_extendida': sanitize(stripRtf(descripcion_extendida)),
                'orden': sanitize(orden),
            }
            rels.append(dict_to_sql(data_base, dcln, table_rel, False))

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'albarancompra', 'alb-compra')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('albarancompra')
        data_base.delete_all('albarancompralinea')
        data_base.delete_where('relacionregistro', 'registro_padre=%d' % n_registro)
        data_base.delete_where('direccion', 'registro=%d' % n_registro)
        data_base.delete_where('ivaregistro', 'registro=%d' % n_registro)
        data_base.delete_where('baseimponible', 'registro=%d' % n_registro)

    relacionados = list()
    data_base.dc_db[table_db] = {}
    n = 0
    for idx in idxs:
        if not idx.strip():
            continue

        n += 1
        if n % 1000 == 0. or n == l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n
        data_base.dc_db[table_db][idx] = n

        (proveedor_, almacen, fecha_albaran, su_n_albaran, referencia, ac_lna, forma_de_pag, descuento_general,
         descuento_pronto_pag, gastos_financieros, importe_gastos_finan, transportista, coste_transporte, observaciones,
         ac_bas, factura, fecha_factura, bruto, neto, impuestos, total, ac_pdp, tipo, hora, iva_fijo, peso_aproximado,
         serie, ac_iva, iva_proveedor, importe_descuento_ge, importe_descuento_pr, codigo_anterior,
         gastos_financieros_d,
         coste_transporte_div, importe_descuento_pr, importe_descuento_ge, total_divisas, impuestos_divisas,
         neto_divisas,
         bruto_divisas, ac_basd, divisa, valor_divisa, sociedad, ac_tz, ac_hist, parte, matricula, tipo_de_portes,
         codigo_di,
         arqueo, cliente, delegacion, proveedor_de_factura, matricula_remolque, parte_de_descarga, conductor,
         estado_email,
         direccion_email_para) = file_rg[idx]

        if factura:
            data_base.dc_db['relaciones_faltan'].append(['relacionregistro', n, n_registro, 'facturas-r', factura])

        if parte:
            data_base.dc_db['relaciones_faltan'].append(['relacionregistro', n, n_registro, 'partes', parte])

        proveedor_de_factura = data_base.dc_db['proveedores'].get(proveedor_de_factura)
        proveedor = data_base.dc_db['proveedores'].get(proveedor_, data_base.dc_db['proveedores']['00001'])
        if proveedor is None:
            raise ValueError(proveedor_, idx)
        iva_proveedor = data_base.dc_db['opciontipo'].get("%d|tipo_iva_proveedor" % iva_proveedor)
        tipo = data_base.dc_db['opciontipo'].get("%s|tipo_albaran_compra" % tipo,
                                                 data_base.dc_db['opciontipo']['C|tipo_albaran_compra'])
        serie = data_base.dc_db['series'].get(serie, data_base.dc_db['series']['000'])
        transportista = data_base.dc_db['transportistas'].get(transportista)
        divisa = data_base.dc_db['divisas'].get(divisa, data_base.dc_db['divisas']['001'])
        sociedad = data_base.dc_db['sociedades'].get(sociedad, data_base.dc_db['sociedades']['00'])
        forma_de_pago = data_base.dc_db['fpago'].get(forma_de_pag, data_base.dc_db['fpago']['000'])
        if forma_de_pago is None:
            raise ValueError(str((forma_de_pag,)))

        matricula_remolque = None
        matricula = None

        i_iva(data_base, ac_iva, n, n_registro, relacionados)
        i_bases(data_base, ac_bas, n, n_registro, relacionados)

        if iva_fijo == '':
            iva_fijo = None
        else:
            iva_fijo = int(iva_fijo)

        if not hora:
            hora = '00:00:00'
        if len(hora) == 5:
            hora += ':00'

        _lineas(ac_lna, n, relacionados)

        dc = {
            u'id': sanitize(n),
            u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'bruto': sanitize(bruto),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'conductor': sanitize(conductor),
            u'coste_transporte': sanitize(coste_transporte),
            u'descuento_general': sanitize(descuento_general),
            u'descuento_ppago': sanitize(descuento_pronto_pag),
            u'email': sanitize(direccion_email_para),
            u'divisa_id': sanitize(divisa),
            u'estado_email': sanitize(estado_email),
            u'fecha_albaran': sanitize(parser_date(fecha_albaran) + ' ' + hora),
            u'modo_pago_id': sanitize(forma_de_pago),
            u'gastos_financieros': sanitize(gastos_financieros),
            u'importe_gastos_finan': sanitize(importe_gastos_finan),
            u'impuestos': sanitize(impuestos),
            u'iva_fijo': sanitize(iva_fijo),
            u'iva_proveedor_id': sanitize(iva_proveedor),
            u'matricula_id': sanitize(matricula),
            u'matricula_remolque_id': sanitize(matricula_remolque),
            u'neto': sanitize(neto),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'peso_aproximado': sanitize(peso_aproximado),
            u'proveedor_id': sanitize(proveedor),
            u'proveedor_facturacion_id': sanitize(proveedor_de_factura),
            u'referencia': sanitize(referencia),
            u'serie_id': sanitize(serie),
            u'sociedad_id': sanitize(sociedad),
            u'su_n_albaran': sanitize(su_n_albaran),
            u'tipo_id': sanitize(tipo),
            u'portes_pagados': sanitize(tipo_de_portes == 'S'),
            u'total': sanitize(total),
            u'transportista_id': sanitize(transportista),
            u'valor_divisa': sanitize(valor_divisa)

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 5000 == 0. or i == l:
            log('', l, i)
        data_base.query(sql_r)


def insert_facturasc(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'facturarecibida', 'facturas-r')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('facturarecibida')
        data_base.delete_where('facturaalbaran', 'registro_factura=%d' % n_registro)
        data_base.delete_where('facturaconcepto', 'registro=%d' % n_registro)
        data_base.delete_where('vencimiento', 'registro=%d' % n_registro)
        data_base.delete_where('baseimponible', 'registro=%d' % n_registro)

    relacionados = list()
    n = 0
    for idx in idxs:
        if not idx.strip():
            continue

        n += 1
        if n % 1000 == 0. or n == l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n

        (proveedor_, su_n_facturar, fecha_factura, fr_dsc, fr_alb, gastos_financieros, coste_transporte, fr_bas, bruto,
         total_descuento_gene, total_descuento_pron, neto, impuestos, total, forma_de_pag, fr_vtos, fr_dpag, pagado,
         pendiente_de_pago, retencion, retencion, cuenta_retencion, fr_ccbas, contabilizada, codigo_de_asiento,
         observaciones,
         referencia_factura, serie, fecha_contabilizado, traspasado, codigo_anterior, iva_fijo, fr_basd, bruto_divisa,
         neto_divisa, total_descuento_gene, total_descuento_pron, impuestos_divisa, total_divisa, pagado_divisa,
         pendiente_divisa, divisa, cambio_divisa, sociedad, gastos_financieros_d, coste_transporte_div,
         importe_retencion_di,
         fr_gaspar) = file_rg[idx]

        proveedor = data_base.dc_db['proveedores'].get(proveedor_, data_base.dc_db['proveedores']['00001'])
        if proveedor is None:
            raise ValueError(proveedor_, idx)
        serie = data_base.dc_db['series'].get(serie, data_base.dc_db['series']['000'])
        divisa = data_base.dc_db['divisas'].get(divisa, data_base.dc_db['divisas']['001'])
        sociedad = data_base.dc_db['sociedades'].get(sociedad, data_base.dc_db['sociedades']['00'])
        forma_de_pago = data_base.dc_db['fpago'].get(forma_de_pag, data_base.dc_db['fpago']['000'])
        if forma_de_pago is None:
            raise ValueError(str((forma_de_pag,)))

        i_bases(data_base, fr_bas, n, n_registro, relacionados)
        i_vencimientos(data_base, fr_vtos, n, n_registro, relacionados)
        i_conceptos(data_base, fr_dsc, n, n_registro, relacionados)
        i_albaranes(data_base, fr_alb, n, n_registro, relacionados)

        for ln in fr_dpag:
            pago, importe = ln[:2]
            data_base.dc_db['relaciones_faltan'].append(['pagofactura', n, n_registro, 'pagos', pago, importe])

        if iva_fijo == '':
            iva_fijo = None
        else:
            iva_fijo = int(iva_fijo)

        iva_fij = None
        if iva_fijo:
            iva_fij = int(iva_fijo)
        dc = {
            u'id': sanitize(n),
            u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'bruto': sanitize(bruto),
            u'cambio_divisa': sanitize(cambio_divisa),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'contabilizada': sanitize(contabilizada == 'S'),
            u'coste_transporte': sanitize(coste_transporte),
            u'cuenta_retencion': sanitize(cuenta_retencion),
            u'divisa_id': sanitize(divisa),
            u'fecha_contabilizado': sanitize(parser_date(fecha_contabilizado)),
            u'fecha_factura': sanitize(parser_date(fecha_factura)),
            u'modo_pago_id': sanitize(forma_de_pago),
            u'gastos_financieros': sanitize(gastos_financieros),
            u'impuestos': sanitize(impuestos),
            u'iva_fijo': sanitize(iva_fij),
            u'neto': sanitize(neto),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'pagado': sanitize(pagado),
            u'pendiente': sanitize(pendiente_de_pago),
            u'proveedor_id': sanitize(proveedor),
            u'referencia_factura': sanitize(referencia_factura),
            u'retencion': sanitize(retencion),
            u'serie_id': sanitize(serie),
            u'sociedad_id': sanitize(sociedad),
            u'su_factura': sanitize(su_n_facturar),
            u'total': sanitize(total),
            u'total_dto_general': sanitize(total_descuento_gene),
            u'total_dto_ppago': sanitize(total_descuento_pron),
            u'traspasado': sanitize(traspasado == 'S')

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 5000 == 0. or i == l:
            log('', l, i)
        data_base.query(sql_r)


def insert_pagos(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'pago', 'pagos')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('pago')
        data_base.delete_all('pagofactura')
        data_base.delete_where('relacionregistro', 'registro_padre=%d' % n_registro)

    relacionados = list()
    n = 0
    for idx in idxs:
        if not idx.strip():
            continue

        n += 1
        if n % 1000 == 0. or n == l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n

        (documento, proveedor_, fecha_emision, fecha_vencimiento, tipo, banco, iban, swift, importe, pg_fras, estad,
         fecha_envio_libre, fecha_pago, remesa, fecha_remesa, contabilizado, descripcion, pg_obs, importe_divisa,
         divisa,
         cambio_divisa, sociedad, codigo_anterior) = file_rg[idx]

        proveedor = data_base.dc_db['proveedores'].get(proveedor_, data_base.dc_db['proveedores']['00001'])
        if proveedor is None:
            raise ValueError(proveedor_, idx)
        tipo = data_base.dc_db['opciontipo'].get(tipo + '|tipo_cartera',
                                                 data_base.dc_db['opciontipo']['O|tipo_cartera'])
        divisa = data_base.dc_db['divisas'].get(divisa, data_base.dc_db['divisas']['001'])
        sociedad = data_base.dc_db['sociedades'].get(sociedad, data_base.dc_db['sociedades']['00'])
        banco = data_base.dc_db['bancos'].get(banco)
        if estad == 'S':
            estad = 'P'
        estado = data_base.dc_db['opciontipo'].get(estad + '|estado_cartera')
        if estado is None:
            raise ValueError(estad)

        if remesa:
            data_base.dc_db['relaciones_faltan'].append(['remesapago', n, n_registro, 'remesapago', remesa])
        if fecha_emision is None:
            fecha_emision = fecha_vencimiento
        dc = {
            u'id': sanitize(n),
            u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'banco_id': sanitize(banco),
            u'cambio_divisa': sanitize(cambio_divisa),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'contabilizado': sanitize(contabilizado == 'S'),
            u'descripcion': sanitize(descripcion),
            u'divisa_id': sanitize(divisa),
            u'documento': sanitize(documento),
            u'estado_id': sanitize(estado),
            u'observaciones': sanitize(stripRtf(pg_obs)),
            u'fecha_emision': sanitize(parser_date(fecha_emision)),
            u'fecha_pago': sanitize(parser_date(fecha_pago)),
            u'fecha_remesa': sanitize(parser_date(fecha_remesa)),
            u'fecha_vencimiento': sanitize(parser_date(fecha_vencimiento)),
            u'iban': sanitize(iban),
            u'importe': sanitize(importe),
            u'proveedor_id': sanitize(proveedor),
            u'sociedad_id': sanitize(sociedad),
            u'tipo_id': sanitize(tipo)

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 5000 == 0. or i == l:
            log('', l, i)
        data_base.query(sql_r)

    l = len(data_base.dc_db['relaciones_faltan'])
    i = 0
    log('Relaciones atrasadas ' + table_db, l)
    for j in range(len(data_base.dc_db['relaciones_faltan']) - 1, -1, -1):
        if data_base.dc_db['relaciones_faltan'][j][0] == 'pagofactura':

            i += 1
            if i % 1000 == 0. or j == 0:
                log('', l, i)

            tabla_db, n, n_registro, tabla_rel, pago, importe = data_base.dc_db['relaciones_faltan'][j]
            pago = data_base.dc_db['pagos'][pago]
            id_ = get_id(data_base, 'pagofactura')
            dc = {
                u'id': sanitize(id_),
                u'empresa_id': empresa_id,
                u'pago_id': sanitize(pago),
                u'factura_id': sanitize(n),
                u'importe': sanitize(importe)

            }
            dict_to_sql(data_base, dc, 'pagofactura')
            del data_base.dc_db['relaciones_faltan'][j]


def insert_remesaspagos(data_base):
    def _lineas(lsta, nt, rels):
        table_rel = 'remesapagolinea'
        orden = 0
        for lnd in lsta:
            orden += 1
            pago, importe, iban, swift = lnd
            pago = data_base.dc_db['pagos'].get(pago)

            id_d = get_id(data_base, table_rel)


            dcln = {
                'id': sanitize(id_d),
                'empresa_id': '1',
                'remesa_id': sanitize(nt),
                'pago_id': sanitize(pago),
                'importe': sanitize(importe),
                'iban': sanitize(iban),
                'orden': sanitize(orden),
            }
            rels.append(dict_to_sql(data_base, dcln, table_rel, False))

    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'remesapago', 'remesas_pagos')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('remesapago')
        data_base.delete_all('remesapagolinea')
        data_base.delete_where('relacionregistro', 'registro_padre=%d' % n_registro)


    relacionados = list()
    data_base.dc_db[table_db]={}
    n=0
    for idx in idxs:
        if not idx.strip():
            continue

        n+=1
        if n % 1000 == 0. or n==l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n
        data_base.dc_db[table_db][idx] = n

        (banco, fecha_remesa, tipo, rmp_cob, total_remesa, gastos_financieros, contabilizada, observaciones,
         fecha_de_subida_a_ba, fecha_de_vencimiento, cuaderno_enviado, serie, codigo_anterior)  = file_rg[idx]

        tipo = data_base.dc_db['opciontipo'].get(tipo+'|tipo_cartera', data_base.dc_db['opciontipo']['O|tipo_cartera'])
        banco = data_base.dc_db['bancos'].get(banco)
        serie = data_base.dc_db['series'].get(serie, data_base.dc_db['series']['000'])

        _lineas(rmp_cob, n, relacionados)

        dc = {
            u'id': sanitize(n),
            u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'banco_id': sanitize(banco),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'contabilizada': sanitize(contabilizada=='S'),
            u'cuaderno': sanitize(cuaderno_enviado),
            u'fecha_subida_banco': sanitize(parser_date(fecha_de_subida_a_ba)),
            u'fecha_vencimiento': sanitize(parser_date(fecha_de_vencimiento)),
            u'fecha_remesa': sanitize(parser_date(fecha_remesa)),
            u'gastos_financieros': sanitize(gastos_financieros),
            u'serie_id': sanitize(serie),
            u'tipo_id': sanitize(tipo),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'total_remesa': sanitize(total_remesa)

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 5000 == 0. or i==l:
            log('', l, i)
        data_base.query(sql_r)

    l = len(data_base.dc_db['relaciones_faltan'])
    i=0
    log('Relaciones atrasadas ' + table_db, l)
    for j in range(len(data_base.dc_db['relaciones_faltan'])-1, -1, -1):
        if data_base.dc_db['relaciones_faltan'][j][0] =='remesapago':

            i += 1
            if i % 1000 == 0. or j == 0:
                log('', l, i)

            tabla_db, n, n_registro, registro_hijo, remesa = data_base.dc_db['relaciones_faltan'][j]
            registro_hijo = data_base.dc_db['tiporegistro'][registro_hijo]
            remesa = data_base.dc_db['remesapago'].get(remesa)
            if remesa is not None:
                id_ = get_id(data_base, 'relacionregistro')
                dc = {
                    u'id': sanitize(id_),
                    u'empresa_id': empresa_id,
                    u'id_hijo': sanitize(remesa),
                    u'registro_hijo': sanitize(registro_hijo),
                    u'id_padre': sanitize(n),
                    u'registro_padre': sanitize(n_registro)

                }
                dict_to_sql(data_base, dc, 'relacionregistro')
            del data_base.dc_db['relaciones_faltan'][j]

def insert_presupuestos(data_base):
    def _lineas(lsta, nt, rels, op_aceptada, idx):

        def _new_opcion(_opcion, _rels, op_ac, idx):

            def _valor(op, ls, df):
                for _l in ls:
                    if _l[0]==op:
                        return  _l[1]
                return df

            table_re = 'presupuestoopcion'
            if _opcion not in opciones.keys():
                id_dd = get_id(data_base, table_re)
                opciones[_opcion] = id_dd
            _orden = len(opciones.keys())

            _divisa = _valor(_opcion, pc_idio, None)
            modo_cobro = _valor(_opcion, pc_fp, None)
            idioma = _valor(_opcion, pc_idio, None)
            descuento_general = _valor(_opcion, pc_dtg, 0.)
            descuento_ppago = _valor(_opcion, pc_dtp, 0.)
            porc_gastos_financieros = _valor(_opcion, pc_gfin, 0.)
            importe_gastos_financieros = _valor(_opcion, pc_dtg, 0.)
            gastos_transporte = _valor(_opcion, pc_ctr, 0.)
            bruto = _valor(_opcion, pc_bru, 0.)
            neto = _valor(_opcion, pc_neto, 0.)
            impuestos = _valor(_opcion, pc_impu, 0.)
            total = _valor(_opcion, pc_ttt, 0.)
            coste_venta = _valor(_opcion, pc_cov, 0.)
            beneficio = _valor(_opcion, pc_bene, 0.)
            porc_beneficio = _valor(pc_pbn, pc_dtg, 0.)
            observaciones = _valor(_opcion, pc_obs, None)
            descripcion = _valor(_opcion, pc_dsc, None)
            iva_fijo = _valor(_opcion, pc_ivadoc, None)
            if iva_fijo == '':
                iva_fijo = None
            else:
                iva_fijo = int(iva_fijo)
            modo_cobro = data_base.dc_db['fpago'].get(modo_cobro, data_base.dc_db['fpago']['001'])
            _divisa = data_base.dc_db['divisas'].get(_divisa, data_base.dc_db['divisas']['001'])
            idioma = data_base.dc_db['idiomas'].get(idioma, data_base.dc_db['idiomas']['001'])
            dc_ln = {
                u'id': sanitize(opciones[_opcion]),
                u'empresa_id': '1',
                u'presupuesto_id': sanitize(nt),
                u'divisa_id': sanitize(divisa),
                u'nombre': sanitize(_opcion),
                u'modo_cobro_id': sanitize(modo_cobro),
                u'idioma_id': sanitize(idioma),
                u'descuento_general': sanitize(descuento_general),
                u'descuento_ppago': sanitize(descuento_ppago),
                u'porc_gastos_financieros': sanitize(porc_gastos_financieros),
                u'importe_gastos_financieros': sanitize(importe_gastos_financieros),
                u'gastos_transporte': sanitize(gastos_transporte),
                u'bruto': sanitize(bruto),
                u'impuestos': sanitize(impuestos),
                u'neto': sanitize(neto),
                u'total': sanitize(total),
                u'coste_venta': sanitize(coste_venta),
                u'beneficio': sanitize(beneficio),
                u'porc_beneficio': sanitize(porc_beneficio),
                u'descripcion': sanitize(stripRtf(descripcion)),
                u'observaciones': sanitize(stripRtf(observaciones)),
                u'iva_fijo': sanitize(iva_fijo),
                u'orden': sanitize(_orden),
                u'aceptada': sanitize(_opcion==op_ac),
            }
            _rels.append(dict_to_sql(data_base, dc_ln, table_re, False))

        table_rel = 'presupuestolinea'
        orden = 0
        opciones = dict()
        for lnd in lsta :
            orden += 1
            (cdar, deno, unidades, precio, preciodivisa, descuento, descuento_2, descuento_3, descuento_4, preciomediocoste,
             preciocosteproveedor, tipo_iva, opcion, partida, capitulo, ref_capitulo, sub_capitulo, ref_sub_capitulo, descompuesto,
             desc_extendida_art, desc_extendida_cap, desc_extendida_scap, info, orden, id_descompuesto,
             id_linea,
             cuotas) = lnd
            cdar = data_base.dc_db['articulos'].get(cdar)
            if cdar is None:
                continue
            id_d = get_id(data_base, table_rel)

            if opcion not in  opciones.keys():
                _new_opcion(opcion, rels, op_aceptada, idx)

            descompuesto = None
            try:
                da = stripRtf(desc_extendida_art)
            except KeyError:
                da = ''
            dcln = {
                u'id': sanitize(id_d),
                u'empresa_id': '1',
                u'presupuesto_id': sanitize(nt),
                u'descompuesto_id': sanitize(descompuesto),
                u'articulo_id': sanitize(cdar),
                u'nombre': sanitize(deno),
                u'unidades': sanitize(unidades),
                u'descuento_1': sanitize(descuento),
                u'descuento_2': sanitize(descuento_2),
                u'descuento_3': sanitize(descuento_3),
                u'descuento_4': sanitize(descuento_4),
                u'precio': sanitize(precio),
                u'precio_medio_coste': sanitize(preciomediocoste),
                u'precio_coste_proveedor': sanitize(preciocosteproveedor),
                u'tipo_iva': sanitize(tipo_iva),
                u'opcion_id': sanitize(opciones[opcion]),
                u'partida': sanitize(partida),
                u'capitulo': sanitize(capitulo),
                u'ref_capitulo': sanitize(ref_capitulo),
                u'sub_capitulo': sanitize(sub_capitulo),
                u'ref_sub_capitulo': sanitize(ref_sub_capitulo),
                u'desc_extendida_art': sanitize(da),
                u'desc_extendida_scap': sanitize(stripRtf(desc_extendida_scap)),
                u'desc_extendida_cap': sanitize(stripRtf(desc_extendida_cap)),
                u'orden': sanitize(orden),
            }
            rels.append(dict_to_sql(data_base, dcln, table_rel, False))
        return opciones

    def _revisiones(nt, presupuesto_inicial, presupuesto_revisado, numero_revision, rels):
        if not numero_revision or not presupuesto_inicial or not presupuesto_revisado:
            return
        print(909)
        table_rel = 'presupuestorevision'
        presupuesto_inicial = data_base.dc_db['presupuestos'][presupuesto_inicial]
        presupuesto_revisado = data_base.dc_db['presupuestos'][presupuesto_revisado]
        id_d = get_id(data_base, table_rel)
        numero_revision = int(numero_revision)
        dcln = {
            u'id': sanitize(id_d),
            u'empresa_id': '1',
            u'presupuesto_inicial_id': sanitize(presupuesto_inicial),
            u'presupuesto_id': sanitize(nt),
            u'presupuesto_revisado_id': sanitize(presupuesto_revisado),
            u'numero_revision': sanitize(numero_revision),
        }
        rels.append(dict_to_sql(data_base, dcln, table_rel, False))


    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'presupuesto', 'presupuestos')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('presupuesto')
        data_base.delete_all('presupuestolinea')
        data_base.delete_all('presupuestoopcion')
        data_base.delete_where('direccion', 'registro=%d' % n_registro)
        data_base.delete_where('ivaregistro', 'registro=%d' % n_registro)
        data_base.delete_where('baseimponible', 'registro=%d' % n_registro)


    relacionados = list()

    n=0
    for idx in idxs:
        if not idx.strip():
            continue

        (cliente_, delegacion_, tipo_cliente, fecha, fecha_vencimiento, serie, almacen, vendedor, atencion_de, probabilidad_exito,
         titulo, estado, estado_anterior, opcion_aceptada, fecha_aceptacion, presupuesto_inicial, revision_actual,
         ultima_revision_pre, pc_fp, pc_dtg, pc_dtp, pc_pgf, pc_gfin, pc_ctr, pc_bru, pc_neto, pc_impu, pc_ttt, pc_cov,
         pc_bene, pc_pbn, pc_pcant, codigo_anterior, observaciones_intern, correcion_importe_va, gastos_generales,
         beneficio_industrial, idi_variable_interna, pc_direc, tipo_de_obra, pc_bas, pc_lna, pc_dsc, pc_obs, pc_iva,
         tipo_de_iva_cliente, pc_ivadoc, pc_hist, presupuesto_revisado, divisa, cambio_divisa, sociedad, pc_basd, pc_gfind,
         pc_ctrd, pc_brud, pc_netod, pc_tttd, pc_impud, tipo_de_divisa_de_en, parte_de_origen_vari, modelo_impresion_var,
         codigo_completo_de_l, porcentaje_gastos_ge, porcentaje_beneficio, gastos_generales_div, beneficios_industria, pc_idio,
         contrato, periodicidad_revisio, tipo_de_periodicidad, texto_impresion_modo)  = file_rg[idx]
        if data_base.dc_db['clientes'].get(cliente_) is None:
            continue

        cliente = data_base.dc_db['clientes'].get(cliente_, data_base.dc_db['clientes']['#0000'])
        if cliente == data_base.dc_db['clientes']['#0000']:
            delegacion_ = '001'

        delegacion = data_base.dc_db['delegaciones'].get(cliente_+delegacion_)
        if delegacion is None:
            raise ValueError(delegacion_, cliente_, idx)

        n+=1
        if n % 75 == 0. or n==l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n

        estado = data_base.dc_db['opciontipo'].get(estado+"|estado_presupuesto")
        estado_anterior = data_base.dc_db['opciontipo'].get(estado_anterior+"|estado_presupuesto")
        tipo_obra = data_base.dc_db['opciontipo'].get(tipo_de_obra+"|tipo_obra")
        tipo_de_iva_cliente = data_base.dc_db['opciontipo'].get(str(tipo_de_iva_cliente)+"|tipo_iva_cliente")
        serie = data_base.dc_db['series'].get(serie, data_base.dc_db['series']['000'])
        sociedad = data_base.dc_db['sociedades'].get(sociedad, data_base.dc_db['sociedades']['00'])
        vendedor = data_base.dc_db['personal'].get(sociedad, data_base.dc_db['personal']['000'])

        _revisiones(n, presupuesto_inicial, presupuesto_revisado, revision_actual, relacionados)

        ops = _lineas(pc_lna, n, relacionados, opcion_aceptada, idx)
        for k in range(len(pc_direc)):
            if k==0:
                fac = False
                env = True
            else:
                fac = True
                env = False
            i_direcciones(data_base, [pc_direc[k]], n, n_registro, relacionados, fac, env)
        i_bases(data_base, pc_bas, n, n_registro, relacionados, ops)
        i_iva(data_base, pc_iva, n, n_registro, relacionados)
        fv = fecha_vencimiento
        if fv is None:
            fv = fecha
        dc = {
            u'id': sanitize(n),
            u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'atencion_de': sanitize(atencion_de),
            u'beneficio_industrial': sanitize(beneficio_industrial),
            u'cambio_divisa': sanitize(cambio_divisa),
            u'cliente_id': sanitize(cliente),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'correccion_importe': sanitize(correcion_importe_va),
            u'delegacion_id': sanitize(delegacion),
            u'estado_id': sanitize(estado),
            u'estado_anterior_id': sanitize(estado_anterior),
            u'fecha': sanitize(parser_date(fecha)),
            u'fecha_aceptacion': sanitize(parser_date(fecha_aceptacion)),
            u'fecha_vencimiento': sanitize(parser_date(fv)),
            u'observaciones_internas': sanitize(stripRtf(observaciones_intern)),
            u'opcion_aceptada': sanitize(opcion_aceptada),
            u'porc_gastos_generales': sanitize(porcentaje_gastos_ge),
            u'probabilidad_exito': sanitize(probabilidad_exito),
            u'serie_id': sanitize(serie),
            u'sociedad_id': sanitize(sociedad),
            u'posible_cliente': sanitize(tipo_cliente=='F'),
            u'tipo_divisa_entrada': sanitize(tipo_de_divisa_de_en==1),
            u'tipo_iva_cliente_id': sanitize(tipo_de_iva_cliente),
            u'tipo_obra_id': sanitize(tipo_obra),
            u'titulo': sanitize(titulo),
            u'vendedor_id': sanitize(vendedor),

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 2000 == 0. or i==l:
            log('', l, i)
        data_base.query(sql_r)

def insert_pedidocliente(data_base):
    def _lineas(lsta, nt, rels):

        table_rel = 'pedidoclientelinea'
        orden = 0
        opciones = dict()
        for lnd in lsta :
            orden += 1

            (cdar, deno, unidades, uds_servidas, precio, preciodivisa, descuento, descuento_2, descuento_3, descuento_4, preciomediocoste,
             estado_linea, tipo_iva, partida, capitulo, ref_capitulo, sub_capitulo, ref_sub_capitulo, descompuesto,
             desc_extendida_art, desc_extendida_cap, desc_extendida_scap, info, id_descompuesto, id_linea) = lnd
            cdar = data_base.dc_db['articulos'].get(cdar)
            if cdar is None:
                continue
            id_d = get_id(data_base, table_rel)
            estado_linea = data_base.dc_db['opciontipo'].get(estado_linea+"|estado_pedido")
            capitulo = data_base.dc_db['capitulos'].get(capitulo)
            sub_capitulo = data_base.dc_db['capitulos'].get(sub_capitulo)
            cdar = data_base.dc_db['articulos'].get(cdar, data_base.dc_db['articulos']['*'])

            descompuesto = None
            try:
                da = stripRtf(desc_extendida_art)
            except KeyError:
                da = ''
            dcln = {
                u'id': sanitize(id_d),
                u'empresa_id': '1',
                u'pedido_id': sanitize(nt),
                u'descompuesto_id': sanitize(descompuesto),
                u'articulo_id': sanitize(cdar),
                u'nombre': sanitize(deno),
                u'unidades': sanitize(unidades),
                u'unidades_servidas': sanitize(uds_servidas),
                u'descuento_1': sanitize(descuento),
                u'descuento_2': sanitize(descuento_2),
                u'descuento_3': sanitize(descuento_3),
                u'descuento_4': sanitize(descuento_4),
                u'precio': sanitize(precio),
                u'precio_coste': sanitize(preciomediocoste),
                u'estado_linea_id': sanitize(estado_linea),
                u'tipo_iva': sanitize(tipo_iva),
                u'partida': sanitize(partida),
                u'capitulo_id': sanitize(capitulo),
                u'referencia_capitulo': sanitize(ref_capitulo),
                u'sub_capitulo_id': sanitize(sub_capitulo),
                u'referencia_sub_cap': sanitize(ref_sub_capitulo),
                u'desc_extendida_art': sanitize(da),
                u'desc_extendida_scap': sanitize(stripRtf(desc_extendida_scap)),
                u'desc_extendida_cap': sanitize(stripRtf(desc_extendida_cap)),
                u'orden': sanitize(orden),
            }
            rels.append(dict_to_sql(data_base, dcln, table_rel, False))
        return opciones


    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'pedidocliente', 'pedidos_cl')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('pedidocliente')
        data_base.delete_all('pedidoclientelinea')
        data_base.delete_where('direccion', 'registro=%d' % n_registro)
        data_base.delete_where('ivaregistro', 'registro=%d' % n_registro)
        data_base.delete_where('baseimponible', 'registro=%d' % n_registro)
        data_base.delete_where('relacionregistro', 'registro_padre=%d' % n_registro)


    relacionados = list()

    n=0
    for idx in idxs:
        if not idx.strip():
            continue

        (cliente_, presupuesto, vendedor, almacen, fecha_pedido, fecha_servir, referencia, pd_lna, descuento_general,
         descuento_pronto_pa, descripcion_pedido, forma_de_pago, observaciones, pd_bas, bruto, total_descuento_gene,
         total_descuento_pron, neto, impuestos, total, transportista, estado, gastos_financieros, total_gastos_financi,
         coste_transporte, delegacion_, coste_de_venta, beneficio, margen, sociedad, traspasado_variable, serie, pd_direc,
         codigo_anterior, pd_hist, cabecera_pedido_test, iva_fijo_documento, opcion_del_presupues, pd_iva, tipo_de_iva_del_clie,
         cliente_final_variab, fecha_traspasado_var, prioridad_variable_i, descuento_as_variabl, aceptado_variable_in, pd_ptr,
         tipo_de_pedido_varia, id_direccion_cliente, pd_basd, total_descuento_gene, total_descuento_pron, neto, bruto, impuestos,
         total, gastos_financieros, coste_transporte, divisa, cambio_divisa, tipo_de_entrada_prec, idioma, tipo_de_portes,
         solicitado_por)  = file_rg[idx]

        if data_base.dc_db['clientes'].get(cliente_) is None:
            continue

        cliente = data_base.dc_db['clientes'].get(cliente_, data_base.dc_db['clientes']['#0000'])
        if cliente == data_base.dc_db['clientes']['#0000']:
            delegacion_ = '001'

        delegacion = data_base.dc_db['delegaciones'].get(cliente_+delegacion_)
        if delegacion is None:
            raise ValueError(delegacion_, cliente_, idx)

        n+=1
        if n % 5 == 0. or n==l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n

        transportista = data_base.dc_db['transportistas'].get(transportista)
        estado = data_base.dc_db['opciontipo'].get(estado+"|estado_pedido")
        tipo_de_iva_del_clie = data_base.dc_db['opciontipo'].get(str(tipo_de_iva_del_clie)+"|tipo_iva_cliente")
        serie = data_base.dc_db['series'].get(serie, data_base.dc_db['series']['000'])
        divisa = data_base.dc_db['divisas'].get(divisa, data_base.dc_db['divisas']['001'])
        idioma = data_base.dc_db['idiomas'].get(idioma, data_base.dc_db['idiomas']['001'])
        almacen = data_base.dc_db['almacenes'].get(almacen, data_base.dc_db['almacenes']['01'])
        forma_de_pago = data_base.dc_db['fpago'].get(forma_de_pago, data_base.dc_db['fpago']['001'])
        sociedad = data_base.dc_db['sociedades'].get(sociedad, data_base.dc_db['sociedades']['00'])
        vendedor = data_base.dc_db['personal'].get(sociedad, data_base.dc_db['personal']['000'])
        if iva_fijo_documento == '':
            iva_fijo_documento = None
        else:
            iva_fijo_documento = int(iva_fijo_documento)


        ops = _lineas(pd_lna, n, relacionados)
        for k in range(len(pd_direc)):
            if k==0:
                fac = False
                env = True
            else:
                fac = True
                env = False
            i_direcciones(data_base, [pd_direc[k]], n, n_registro, relacionados, fac, env)
        i_bases(data_base, pd_bas, n, n_registro, relacionados, ops)
        i_iva(data_base, pd_iva, n, n_registro, relacionados)
        i_relacion(data_base, [presupuesto], 'presupuesto', n, n_registro, relacionados, 'presupuestos')

        dc = {
            u'id': sanitize(n),u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'almacen_id': sanitize(almacen),
            u'cliente_id': sanitize(cliente),
            u'delegacion_id': sanitize(delegacion),
            u'sociedad_id': sanitize(sociedad),
            u'serie_id': sanitize(serie),
            u'divisa_id': sanitize(divisa),
            u'estado_id': sanitize(estado),
            u'idioma_id': sanitize(idioma),
            u'modo_cobro_id': sanitize(forma_de_pago),
            u'vendedor_id': sanitize(vendedor),
            u'transportista_id': sanitize(transportista),

            u'tipo_iva_cliente_id': sanitize(tipo_de_iva_del_clie),
            u'beneficio': sanitize(beneficio),
            u'bruto': sanitize(bruto),
            u'cambio_divisa': sanitize(cambio_divisa),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'coste_venta': sanitize(coste_de_venta),
            u'coste_transporte': sanitize(coste_transporte),
            u'descripcion': sanitize(stripRtf(descripcion_pedido)),
            u'descuento_general': sanitize(descuento_general),
            u'descuento_ppago': sanitize(descuento_pronto_pa),
            u'fecha_pedido': sanitize(parser_date(fecha_pedido)),
            u'fecha_servir': sanitize(parser_date(fecha_servir)),
            u'porc_gastos_financieros': sanitize(gastos_financieros),
            u'impuestos': sanitize(impuestos),
            u'iva_fijo': sanitize(iva_fijo_documento),
            u'margen': sanitize(margen),
            u'neto': sanitize(neto),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'referencia': sanitize(referencia),
            u'solicitado_por': sanitize(solicitado_por),
            u'entrada_divisa_local': sanitize(tipo_de_entrada_prec==1),
            u'portes_pagados': sanitize(tipo_de_portes!='D'),
            u'total': sanitize(total),
            u'imp_gastos_financieros': sanitize(total_gastos_financi),

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 30 == 0. or i==l:
            log('', l, i)
        data_base.query(sql_r)

def insert_albaranventa(data_base):
    def _lineas(lsta, nt, rels):

        table_rel = 'albaranventalinea'
        orden = 0
        opciones = dict()
        for lnd in lsta :
            orden += 1

            (cdar, deno, unidades, precio, preciodivisa, descuento, descuento_2, descuento_3, descuento_4, preciomediocoste,
             tipo_iva, almacen_, partida, capitulo, ref_capitulo, sub_capitulo, ref_sub_capitulo, nseries, desc_extendida_art,
             desc_extendida_cap, desc_extendida_scap, info, lotes, uds_abonadas, descompuesto, id_descompuesto, id_linea,
             di, libre) = lnd
            cdar = data_base.dc_db['articulos'].get(cdar)
            if cdar is None:
                continue
            id_d = get_id(data_base, table_rel)
            capitulo = data_base.dc_db['capitulos'].get(capitulo)
            sub_capitulo = data_base.dc_db['capitulos'].get(sub_capitulo)
            almacen_ = data_base.dc_db['almacenes'].get(almacen_)
            cdar = data_base.dc_db['articulos'].get(cdar, data_base.dc_db['articulos']['*'])

            descompuesto = None
            try:
                da = stripRtf(desc_extendida_art)
            except KeyError:
                da = ''
            dcln = {
                u'id': sanitize(id_d),
                u'empresa_id': '1',
                u'albaran_id': sanitize(nt),
                u'descompuesto_id': sanitize(descompuesto),
                u'articulo_id': sanitize(cdar),
                u'almacen_id': sanitize(almacen_),
                u'nombre': sanitize(deno),
                u'unidades_abonadas': sanitize(uds_abonadas),
                u'unidades': sanitize(unidades),
                u'descuento_1': sanitize(descuento),
                u'descuento_2': sanitize(descuento_2),
                u'descuento_3': sanitize(descuento_3),
                u'descuento_4': sanitize(descuento_4),
                u'precio': sanitize(precio),
                u'precio_coste': sanitize(preciomediocoste),
                u'tipo_iva': sanitize(tipo_iva),
                u'partida': sanitize(partida),
                u'capitulo_id': sanitize(capitulo),
                u'referencia_capitulo': sanitize(ref_capitulo),
                u'sub_capitulo_id': sanitize(sub_capitulo),
                u'referencia_sub_cap': sanitize(ref_sub_capitulo),
                u'desc_extendida_art': sanitize(da),
                u'desc_extendida_scap': sanitize(stripRtf(desc_extendida_scap)),
                u'desc_extendida_cap': sanitize(stripRtf(desc_extendida_cap)),
                u'orden': sanitize(orden),
            }
            rels.append(dict_to_sql(data_base, dcln, table_rel, False))
        return opciones


    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'albaranventa', 'alb-venta')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('albaranventa')
        data_base.delete_all('albaranventalinea')
        data_base.delete_where('direccion', 'registro=%d' % n_registro)
        data_base.delete_where('ivaregistro', 'registro=%d' % n_registro)
        data_base.delete_where('baseimponible', 'registro=%d' % n_registro)
        data_base.delete_where('relacionregistro', 'registro_padre=%d' % n_registro)


    relacionados = list()

    n=0
    for idx in idxs:
        if not idx.strip():
            continue

        (pedido_de_cliente, cliente_, delegacion_, vendedor, almacen, fecha, tipo_albaran, referencia, retirado_por, av_lna,
         forma_de_pago, descuento_general, descuento_pronto_pa, gastos_financieros, gastos_financieros, bultos, peso,
         transportista, portes, observaciones, av_bas, numero_factura, fecha_factura, bruto, total_descuento_gene,
         total_descuento_pron, neto, impuestos, total, coste_venta, beneficio, margen_de_beneficio, numero_contrato,
         tipo_contrato, av_basd, tipo_de_portes, parte_de_decantado, solicitado_por, matricula, codigo_di, divisa,
         cambio_divisa, sociedad, matricla_remolque, conductor, cif_conductor, maticula_isotank, ano_del_periodo_alba,
         gastos_financieros_d, bruto_divisas, neto_divisas, total_divisas, impuestos_divisas, costes_transporte_di,
         pendiente_divisas, cobrado_divisas, arqueo, libre, libre1, libre2, libre3, libre4, libre5, mes_del_periodo_alba,
         libre6, libre7, libre8, libre9, obra, libre10, numero_parte, tipo_de_entrada_de_d, hora, serie, av_direc, codigo_anterior,
         av_hist, cobrado, av_cobav, pendiente, av_ac, iva_documento, validacion_del_certi, av_vtos, av_iva, iva_cliente,
         albaran_al_que_abona, av_avre, av_ptr, idioma, av_tz)  = file_rg[idx]

        if data_base.dc_db['clientes'].get(cliente_) is None:
            continue

        cliente = data_base.dc_db['clientes'].get(cliente_, data_base.dc_db['clientes']['#0000'])
        if cliente == data_base.dc_db['clientes']['#0000']:
            delegacion_ = '001'

        delegacion = data_base.dc_db['delegaciones'].get(cliente_+delegacion_)
        if delegacion is None:
            raise ValueError(delegacion_, cliente_, idx)

        n+=1
        if n % 750 == 0. or n==l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n

        transportista = data_base.dc_db['transportistas'].get(transportista)
        iva_cliente = data_base.dc_db['opciontipo'].get(str(iva_cliente)+"|tipo_iva_cliente")
        tipo_albaran = data_base.dc_db['opciontipo'].get(str(tipo_albaran)+"|tipo_albaran_venta")
        serie = data_base.dc_db['series'].get(serie, data_base.dc_db['series']['000'])
        divisa = data_base.dc_db['divisas'].get(divisa, data_base.dc_db['divisas']['001'])
        idioma = data_base.dc_db['idiomas'].get(idioma, data_base.dc_db['idiomas']['001'])
        almacen = data_base.dc_db['almacenes'].get(almacen, data_base.dc_db['almacenes']['01'])
        forma_de_pago = data_base.dc_db['fpago'].get(forma_de_pago, data_base.dc_db['fpago']['001'])
        sociedad = data_base.dc_db['sociedades'].get(sociedad, data_base.dc_db['sociedades']['00'])
        vendedor = data_base.dc_db['personal'].get(sociedad, data_base.dc_db['personal']['000'])
        if iva_documento == '':
            iva_documento = None
        else:
            iva_documento = int(iva_documento)


        _lineas(av_lna, n, relacionados)
        for k in range(len(av_direc)):
            if k==0:
                fac = False
                env = True
            else:
                fac = True
                env = False
            i_direcciones(data_base, [av_direc[k]], n, n_registro, relacionados, fac, env)
        i_bases(data_base, av_bas, n, n_registro, relacionados)
        i_iva(data_base, av_iva, n, n_registro, relacionados)
        #av_cobav, av_ac, av_vtos, av_avre
        '''
            u'': sanitize(arqueo),
            u'': sanitize(albaran_al_que_abona),
            u'': sanitize(numero_contrato),
            u'': sanitize(numero_factura),
            u'': sanitize(numero_parte),
            u'': sanitize(pedido_de_cliente),
        '''
        if not hora:
            hora = '00:00:00'
        dc = {
            u'id': sanitize(n),
            u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'almacen_id': sanitize(almacen),
            u'beneficio': sanitize(beneficio),
            u'bruto': sanitize(bruto),
            u'bultos': sanitize(bultos),
            u'cambio_divisa': sanitize(cambio_divisa),
            u'cliente_id': sanitize(cliente),
            u'cobrado': sanitize(cobrado),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'coste_venta': sanitize(coste_venta),
            u'delegacion_id': sanitize(delegacion),
            u'dto_general': sanitize(descuento_general),
            u'dto_ppago': sanitize(descuento_pronto_pa),
            u'divisa_id': sanitize(divisa),
            u'fecha': sanitize(parser_date(fecha)+ ' ' + hora),
            u'modo_cobro_id': sanitize(forma_de_pago),
            u'gastos_financieros': sanitize(gastos_financieros),
            u'idioma_id': sanitize(idioma),
            u'impuestos': sanitize(impuestos),
            u'iva_cliente_id': sanitize(iva_cliente),
            u'iva_documento': sanitize(iva_documento),
            u'margen_beneficio': sanitize(margen_de_beneficio),
            u'neto': sanitize(neto),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'pendiente': sanitize(pendiente),
            u'peso': sanitize(peso),
            u'portes': sanitize(portes),
            u'referencia': sanitize(referencia),
            u'retirado_por': sanitize(retirado_por),
            u'serie_id': sanitize(serie),
            u'sociedad_id': sanitize(sociedad),
            u'solicitado_por': sanitize(solicitado_por),
            u'tipo_albaran_id': sanitize(tipo_albaran),
            u'entrada_divisa_local': sanitize(tipo_de_entrada_de_d==1),
            u'portes_pagados': sanitize(tipo_de_portes != 'D'),
            u'total': sanitize(total),
            u'transportista_id': sanitize(transportista),
            u'vendedor_id': sanitize(vendedor)
        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 1500 == 0. or i==l:
            log('', l, i)
        data_base.query(sql_r)

def insert_facturasv(data_base):
    table_db, table_erp, file_rg, idxs, l, n_registro = get_data(data_base, 'facturaemitida', 'facturas-e')
    log(table_erp, len(idxs))

    if reset:
        data_base.delete_all('facturaemitida')
        data_base.delete_where('facturaalbaran', 'registro_factura=%d' % n_registro)
        data_base.delete_where('facturaconcepto', 'registro=%d' % n_registro)
        data_base.delete_where('vencimiento', 'registro=%d' % n_registro)
        data_base.delete_where('baseimponible', 'registro=%d' % n_registro)

    relacionados = list()
    n = 0
    for idx in idxs:
        if not idx.strip():
            continue

        n += 1
        if n % 1000 == 0. or n == l:
            log(idx, l, n)
        data_base.dc_db[table_erp][idx] = n

        (cliente_, almacen, vendedor, fecha_factura, fe_dsc, fe_alb, gastos_financieros, coste_transporte, fe_bas, bruto,
         total_descuento_gene, total_descuento_pron, neto, impuestos, total, forma_de_pago, fe_vtos, fe_dcob,
         pendiente_de_cobro, cobrado, retencion, importe_retencion, cuenta_retencion, libre, fe_ccbas, contabilizada,
         asiento_especial, coste_venta, beneficio, margen, cobrador, solicitado_por, libre, libre, libre, libre, libre,
         observaciones, libre, libre, libre, obra, referencia_factura, enviado_e_mail, traspasada_factura_e, serie, fe_direc,
         codigo_anterior, delegacion, fe_hist, fe_rect, facturas_de_diseno_l, concepto_factura_dis, fe_facrec, fe_cer, libre,
         divisa, cambio_divisa, sociedad, bruto_divisa, neto_divisa, impuestos_divisa, total_divisa, total_descuento_gene,
         total_descuento_pron, coste_transporte_div, gastos_financieros_d, pendiente_divisa, cobrado_divisa, retencion_divisa,
         fe_basd, idioma, comision_general_ven) = file_rg[idx]

        cliente = data_base.dc_db['clientes'].get(cliente_, data_base.dc_db['clientes']['00001'])
        if cliente is None:
            raise ValueError(cliente_, idx)

        idioma = data_base.dc_db['idiomas'].get(idioma, data_base.dc_db['idiomas']['001'])
        serie = data_base.dc_db['series'].get(serie, data_base.dc_db['series']['000'])
        divisa = data_base.dc_db['divisas'].get(divisa, data_base.dc_db['divisas']['001'])
        sociedad = data_base.dc_db['sociedades'].get(sociedad, data_base.dc_db['sociedades']['00'])
        forma_de_pago = data_base.dc_db['fpago'].get(forma_de_pago, data_base.dc_db['fpago']['000'])
        vendedor = data_base.dc_db['personal'].get(vendedor, data_base.dc_db['personal']['001'])
        cobrador = data_base.dc_db['personal'].get(cobrador)
        if forma_de_pago is None:
            raise ValueError(str((forma_de_pago,)))

        i_bases(data_base, fe_bas, n, n_registro, relacionados)
        i_vencimientos(data_base, fe_vtos, n, n_registro, relacionados)
        i_conceptos(data_base, fe_dsc, n, n_registro, relacionados)
        i_albaranes(data_base, fe_alb, n, n_registro, relacionados, 'albaranventa')

        for ln in fe_dcob:
            cobro, importe = ln[:2]
            data_base.dc_db['relaciones_faltan'].append(['cobrofactura', n, n_registro, 'cobros', cobro, importe])


        dc = {
            u'id': sanitize(n),
            u'codigo': sanitize(idx),
            u'empresa_id': empresa_id,
            u'beneficio': sanitize(beneficio),
            u'bruto': sanitize(bruto),
            u'cambio_divisa': sanitize(cambio_divisa),
            u'cliente_id': sanitize(cliente),
            u'cobrado': sanitize(cobrado),
            u'cobrador_id': sanitize(cobrador),
            u'codigo_anterior': sanitize(codigo_anterior),
            u'comision_general': sanitize(comision_general_ven),
            u'contabilizada': sanitize(contabilizada=='S'),
            u'portes': sanitize(coste_transporte),
            u'coste_venta': sanitize(coste_venta),
            u'cuenta_retencion': sanitize(cuenta_retencion),
            u'divisa_id': sanitize(divisa),
            u'enviado_email': sanitize(enviado_e_mail=='S'),
            u'fecha_factura': sanitize(parser_date(fecha_factura)),
            u'modo_cobro_id': sanitize(forma_de_pago),
            u'gastos_financieros': sanitize(gastos_financieros),
            u'idioma_id': sanitize(idioma),
            u'importe_retencion': sanitize(importe_retencion),
            u'impuestos': sanitize(impuestos),
            u'margen': sanitize(margen),
            u'neto': sanitize(neto),
            u'observaciones': sanitize(stripRtf(observaciones)),
            u'pendiente': sanitize(pendiente_de_cobro),
            u'referencia_factura': sanitize(referencia_factura),
            u'porc_retencion': sanitize(retencion),
            u'serie_id': sanitize(serie),
            u'sociedad_id': sanitize(sociedad),
            u'solicitado_por': sanitize(solicitado_por),
            u'total': sanitize(total),
            u'total_dto_general': sanitize(total_descuento_gene),
            u'total_dto_ppago': sanitize(total_descuento_pron),
            u'vendedor_id': sanitize(vendedor)

        }

        dict_to_sql(data_base, dc, table_db)

    l = len(relacionados)
    log('Relacionados ' + table_db, l)
    i = 0
    for sql_r in relacionados:
        i += 1
        if i % 5000 == 0. or i == l:
            log('', l, i)
        data_base.query(sql_r)
