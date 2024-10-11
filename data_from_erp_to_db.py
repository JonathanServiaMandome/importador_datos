# -*- coding: utf-8 -*-
import codecs
import json
import os
import unicodedata
from numpy import unicode

from functions import *
import pyodbc

class Database:
    host = "127.0.0.1"
    user = "postgres"
    passwd = "admin"
    db = "sat_db"

    def __init__(self):
        self.connection = pyodbc.connect(
            "DRIVER={PostgreSQL Unicode(x64)}; "
            "DATABASE=sat_db;     "
            "UID=postgres; "
            "PWD=admin; "
            "SERVER=localhost; "
            "PORT=5432;",
            autocommit=True)

        self.cursor = self.connection.cursor()


    def query(self, q):
        cursor = self.cursor
        cursor.execute(q)


    def delete(self, table):
        cursor = self.cursor
        cursor.execute('DELETE FROM empresas_%s' % table.lower())

    def insert(self, table, campos, values):
        cursor = self.cursor
        x = ', '.join(values)

        q = "INSERT INTO empresas_%s (%s) values (%s)" % (table.lower(), ', '.join(campos), x)
        #print(q)
        self.cursor.execute(q)
        #self.connection.commit()

    def __del__(self):
        self.connection.close()

def import_lvl_1(data_base):
    data = [
        ['articulos_formatos', 'ArticulosFormatos'],
        ['categorias_clientes','CategoriasClientes'],
        ['departamentos','Departamentos'],
        ['divisas','Divisas'],
        ['emails','Emails'],
        ['idiomas','Idiomas'],
        ['marcas','Marcas'],
        ['marcasRiesgo','MarcasRiesgo'],
        ['rol_horario_laboral','CostesEmpleados'],
        ['rutas','Rutas'],
        ['sectores','SectoresEmpresariales'],
        ['series','Series'],
        ['sociedades','Sociedades'],
        ['swift','Swift'],
        ['tarifas','Tarifas'],
        ['zonas','Zonas'],
    ]
    for ln in data:
        ar,AR = ln
        ruta = "D:/Server/Companies/jonathan/tesein/EJA/"+ar
        file = bkopen(ruta, huf=0)
        data_base.delete_all(AR)
        with open(os.getcwd()+'\\dc_n1.json') as f:
            js = json.loads(f.read())

        for idx in file.keys():
            campos = []
            values = []
            ls=[]

            values.append("'" + idx + "'")
            campos.append('codigo')

            for i in range(len(file[idx])):
                j = str(i)
                if j not in js[AR].keys():
                    continue

                name, fmt = js[AR][j]
                value = file[idx][i]

                if ar=='series' and name==u'observaciones':
                    value = '\n'.join(value)

                if fmt=='d':
                   value =  Num_aFecha(value)
                elif fmt in ['l', '%']:
                    if type(value) == list:
                        raise ValueError((name, fmt))
                    value = "'"+value.decode('latin1')+"'"
                else:
                    value = str(value)

                values.append(value)
                campos.append(name)
            values.append("1")
            campos.append('empresa_id')
            data_base.insert(AR, campos, values)

def import_types(data_base):
    data = [
        ['llamadas_tipos', 'Llamadas_tipos', 'llamadas'],
        ['costes_descompuestos','CostesDescompuestos','costes_descompuestos'],
        ['tnoconformidad','Tnoconformidad', 'no_conformidades'],
        ['tipo_gastos','Tipo_gastos', 'gastos'],
        ['partes_prioridades','Partes_prioridades', 'prioridades_partes'],
        ['contratos_ss','Contratos_ss', 'contratos_ss'],
        ['obras_tipos','Obras_tipos', 'obras'],
        ['contratos_clases','Contratos_clases', 'contratos'],
        ['fcli_acs','AccionesComerciales','acciones_comerciales'],
        ['origen_con','OrigenConformidad','origen_conformidad'],
    ]

    data_base.delete_all('tipos')
    for ln in data:
        ar, AR, tabla = ln
        ruta = "D:/Server/Companies/jonathan/tesein/EJA/"+ar
        file = bkopen(ruta, huf=0)
        with open(os.getcwd()+'\\dc_tipos.json') as f:
            js = json.loads(f.read())

        for idx in file.keys():
            campos = list()
            values = list()

            values.append("'" + idx + "'")
            campos.append('codigo')

            for i in range(len(file[idx])):
                j = str(i)
                if j not in js[AR].keys():
                    continue

                name, fmt, rel = js[AR][j]
                value = file[idx][i]

                if fmt=='d':
                   value =  Num_aFecha(value)
                elif fmt in ['l', '%']:
                    if type(value) == list:
                        raise ValueError((name, fmt))
                    value = "'"+value.decode('latin1')+"'"
                else:
                    value = str(value)

                values.append(value)
                campos.append(name)
            values.append("'" + tabla + "'")
            campos.append('tabla')
            values.append("1")
            campos.append('empresa_id')
            data_base.insert('tipos', campos, values)

def import_types_esp(data_base):
    data = [
       # ['acciones_tipos','AccionesTipos', []],
        ['partes_tipos', 'PartesTipos', 'PartesTiposTecnicos'],
    ]
    data_rel = list()
    for ln in data:
        ar, AR, tabla_rel = ln
        data_base.delete_all(AR)
        ruta = "D:/Server/Companies/jonathan/tesein/EJA/"+ar
        file = bkopen(ruta, huf=0)
        with open(os.getcwd()+'\\dc_tipos.json') as f:
            js = json.loads(f.read())

        for idx in file.keys():
            campos = list()
            values = list()

            values.append("'" + idx + "'")
            campos.append('codigo')
            rg = list(file[idx])

            for i in range(len(file[idx])):
                j = str(i)
                if j not in js[AR].keys():
                    continue

                name, fmt, rel = js[AR][j]
                value = file[idx][i]

                if i == 2 and AR == 'PartesTipos':
                    q = "SELECT id FROM empresas_accionestipos WHERE codigo='%s'" % file[idx][2]
                    res = data_base.query(q)
                    file[idx][2] = res[0][0]
                    fmt='i'
                if i == 4 and AR == 'PartesTipos':
                    for tec in file[idx][i]:
                        data_rel.append(
                            [
                                tabla_rel,
                                ['tecnico', 'empresa_id', 'parte_tipo_id'],
                                ["'" + tec + "'", "1","'" + idx + "'"],
                                AR
                            ]
                        )

                if fmt=='d':
                   value =  Num_aFecha(value)
                elif fmt in ['l', '%']:
                    if type(value) == list:
                        raise ValueError((name, fmt))
                    value = "'" + value.decode('latin1') + "'"
                else:
                    value = str(value)

                values.append(value)
                campos.append(name)
            values.append("1")
            campos.append('empresa_id')
            data_base.insert(AR, campos, values)

    for data in data_rel:
        tecnico, empresa_id, parte_tipo = data[2]
        q = "SELECT id FROM empresas_%s WHERE codigo=%s" % (data[3].lower(), parte_tipo)
        res = data_base.query(q)
        data[2][2] = res[0][0]
        data_base.insert( tecnico, empresa_id, data)


if __name__ =='__main__':
    db = Database()
    lineas = codecs.open(os.getcwd()+'\\import1.sql', 'r', 'utf8').readlines()
    for sql in lineas:
        sql = sql.strip()
        try:
            db.query(sql+' ON CONFLICT (id) DO NOTHING')
        except Exception as e:
            raise ValueError(sql, e)







