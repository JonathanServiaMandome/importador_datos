# -*- coding: utf-8 -*-
import codecs
import json
import os

import pyodbc

from functions import Num_aFecha, stripRtf


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

        if level == 1:
            self.dc_parser = {
                'id_relaciones': 1,
                'id_direcciones': 1,
                'id_tecnicos': 1,
                'id_caracteristica': 1,
                'id_art_desc':1,
                'id_traducciones':1,
                'id_caracteristicas_registros':1,
                'id_fechas_registros':1,
                'id_checklist_registros':1,
                'id_checklists_lineas':1,
                'id_valor_predefinido':1,
                'id_contactos':1,
                'id_stock':1,
                'id_articulosfacturar':1,
                'id_modeloscert':1,
                'id_iban':1,
                'id_checksecundario':1,
            }
        elif level == 2:
            path=os.getcwd()+'\\import1.json'
            js = open(path, 'rb').read()
            self.dc_db = json.loads(js)
            self.dc_parser = {
                'id_relaciones': self.dc_db['id_relaciones'],
                'id_direcciones': self.dc_db['id_direcciones'],
                'id_tecnicos': self.dc_db['id_tecnicos'],
                'id_art_desc': self.dc_db['id_art_desc'],
                'id_caracteristica': self.dc_db['id_caracteristica'],
                'id_traducciones': self.dc_db['id_traducciones'],
                'id_caracteristicas_registros': self.dc_db['id_caracteristicas_registros'],
                'id_fechas_registros': self.dc_db['id_fechas_registros'],
                'id_checklist_registros': self.dc_db['id_checklist_registros'],
                'id_checklists_lineas': self.dc_db['id_checklists_lineas'],
                'id_valor_predefinido': self.dc_db['id_valor_predefinido'],
                'id_contactos': self.dc_db.get('id_contactos', 1),
                'id_stock': self.dc_db.get('id_stock', 1),
                'id_articulosfacturar': self.dc_db.get('id_articulosfacturar', 1),
                'id_modeloscert': self.dc_db.get('id_modeloscert', 1),
                'id_iban': self.dc_db.get('id_iban', 1),
                'id_checksecundario': self.dc_db.get('id_checksecundario', 1),
            }


    def close(self):
        path=os.getcwd() +'\\import%d.json' % self.level
        codecs.open(path, 'w', encoding="utf-8").write(json.dumps(self.dc_parser, indent=4))
        path=os.getcwd() +'\\import%d.sql' % self.level
        tx = '\n'.join(self.deletes)
        tx += '\n'
        tx += '\n'.join(self.inserts)
        codecs.open(path, 'w', encoding="utf-8").write(tx)
        #self.connection.close()

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

def _join(ls):
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

def _fechas(data_base, ls_fechas, index, n_registro, relacionados, dcdb):
    table_rel = 'fechasregistros'
    for ln_fec in ls_fechas:
        cd_fec, periodicidad_f, tipo_periodicidad_f = ln_fec

        id_fec = dcdb['nseries_fechas'][cd_fec]
        id_ = data_base.dc_parser['id_fechas_registros']
        data_base.dc_parser['id_fechas_registros'] += 1
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

def _caracteristicas(data_base, ls_car, index, n_registro, relacionados, rgs_caracteristicas, dcdb):
    table_rel = 'caracteristicasregistros'
    for cd_car in ls_car:
        if type(cd_car)==str:
            if rgs_caracteristicas.get(cd_car) is None:
                raise ValueError(cd_car, rgs_caracteristicas.keys())
            rg_car = rgs_caracteristicas[cd_car]
            tipo_valor = rg_car[1][0].upper()
        else:
            cd_car, tipo_valor, texto, numerico, fecha = cd_car[:5]

        id_car = dcdb['nseries_caracteristica'].get(cd_car)
        if id_car is None:
            continue
        id_ = data_base.dc_parser['id_caracteristicas_registros']
        data_base.dc_parser['id_caracteristicas_registros'] += 1

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

def _traducciones(data_base, ls_tra, index, n_registro, traducciones, dcdb, ext=False):
    if index==18:
        return
    table_rel = 'traducciones'
    for ln_tra in ls_tra:
        cd_tra, nom = ln_tra[:2]
        if ext:
            nom = stripRtf(nom)
        try:
            id_tra = dcdb['idiomas'][cd_tra]
        except:
            raise ValueError(ls_tra, ext, index)

        id_ = data_base.dc_parser['id_traducciones']
        data_base.dc_parser['id_traducciones'] += 1
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

        traducciones.append(sql_ln)

def _checks(data_base, ls_check, index, n_registro, relacionados, dcdb):
    table_rel = 'checklistregistros'
    for ln_check in ls_check:
        cd_check, tp_accion, idio = ln_check
        id_check = dcdb['checklists'][cd_check]
        id_idi = dcdb['idiomas'].get(idio)
        tipo_accion_id = dcdb['acciones_tipos'].get(tp_accion)

        id_ = data_base.dc_parser['id_checklist_registros']
        data_base.dc_parser['id_checklist_registros'] += 1
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

def _direcciones(data_base, ls_direcciones, index, num_registro, relacionados, dcdb, facturacion, envio):
    for ln_direccion in ls_direcciones:
        domicilio, codigo_postal, poblacion, provincia, pais = ln_direccion[:5]

        id_ = data_base.dc_parser['id_direcciones']
        data_base.dc_parser['id_direcciones'] += 1
        provincia = dcdb['provincias'].get(codigo_postal[:2])
        codigo_postal = dcdb['postal'].get(codigo_postal)
        pais = dcdb['paises'].get(pais)
        table_rel = 'direcciones'
        dc_ln = {
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

def _contactos(data_base, ls_contactos, index, num_registro, relacionados):
    for ln_contacto in ls_contactos:
        if len(ln_contacto) == 7:
            nombre, cargo, telefono, email, defe, cif, fax = ln_contacto[:7]
            defe = defe=='S'
        else:
            nombre, cargo, telefono, email = ln_contacto[:4]
            defe = False
            cif = None
            fax = None
        id_ = data_base.dc_parser['id_contactos']
        data_base.dc_parser['id_contactos']+=1
        table_rel = 'contactos'
        dc_ln = {
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

def _iban(data_base, ls_contactos, index, num_registro, relacionados):
    table_rel = 'ibanregistros'
    for ln_contacto in ls_contactos:
        iban, swift, defe = ln_contacto[:3]

        id_ = data_base.dc_parser['id_iban']
        data_base.dc_parser['id_iban']+=1
        dc_ln = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'iban': sanitize(iban),
            'defecto': sanitize(defe=='S'),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
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

def _relacion(data_base, ls, tabla_hijo, padre, p_registro, relacionados, dcdb):
    table_rel = 'relacionregistros'
    h_registro = dcdb['tiporegistros'][tabla_hijo]
    for ln in ls:
        idh = None
        defecto = False
        if type(ln) == str:
            idh = dcdb[tabla_hijo].get[ln]
            if idh is None:
               raise ValueError(str(ln))
        elif type(ln) == str:
           idh = dcdb[tabla_hijo].get[ln[0]]
           defecto = ln[1] == 'S'
           if idh is None:
               raise ValueError(str(ln))
        if idh is None:
            continue

        id_ = data_base.dc_parser['id_relaciones']
        data_base.dc_parser['id_relaciones']+=1
        dc_ln = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'registro_hijo': sanitize(h_registro),
            'id_hijo': sanitize(idh),
            'registro_padre': sanitize(p_registro),
            'id_padre': sanitize(padre),
            'defecto': sanitize(defecto),
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

def _modeloscertificado(data_base, ls_mod, index, num_registro, modelos_ls, dcdb):
    table_rel = 'modeloscertificadoregistros'
    for ln_mod in ls_mod:
        idioma, tipo_contrato, tipo_parte, modelo_certificado = ln_mod
        idioma = dcdb['idiomas'].get(idioma)
        modelo_certificado = dcdb['partes_certificado_modelo'].get(modelo_certificado)
        tipo_parte = dcdb['partes_tipos'].get(tipo_parte)
        tipo_contrato = dcdb['contratos_clases'].get(tipo_contrato)


        id_ = data_base.dc_parser['id_modeloscert']
        data_base.dc_parser['id_modeloscert']+=1
        dc_ln = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'idioma_id': sanitize(idioma),
            'modelo_certificado_id': sanitize(modelo_certificado),
            'tipo_parte_id': sanitize(tipo_parte),
            'tipo_contrato_id': sanitize(tipo_contrato),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
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

        modelos_ls.append(sql_ln)

def _checksecundario(data_base, ls_mod, index, num_registro, modelos_ls, dcdb):
    table_rel = 'checklistsecundarioregistros'
    for ln_mod in ls_mod:
        idioma, modelo, checksec = ln_mod
        checklists_secundario = dcdb['checklists'].get(checksec)
        modelo_certificado = dcdb['partes_certificado_modelo'].get(modelo)
        idioma_i = dcdb['idiomas'].get(idioma)

        id_ = data_base.dc_parser['id_checksecundario']
        data_base.dc_parser['id_checksecundario']+=1
        dc_ln = {
            'id': sanitize(id_),
            'empresa_id': '1',
            'idioma_id': sanitize(idioma_i),
            'modelo_certificado_id': sanitize(modelo_certificado),
            'checklists_secundario_id': sanitize(checklists_secundario),
            'id_registro': sanitize(index),
            'registro': sanitize(num_registro),
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

        modelos_ls.append(sql_ln)
