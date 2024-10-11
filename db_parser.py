# -*- coding: utf-8 -*-
import json
import os

from functions import *


def get_level(data_base):
	files_lvl_1 = list()
	files_lvl_2 = list()
	files_lvl_3 = list()
	dicts_ignore = [
		#Tipos
		'llamadas_tipos',
		'acciones_tipos',
		'contratos_clases',
		'contratos_ss',
		'llamadas_tipos',
		'obras_tipos',
		'partes_prioridades',
		'partes_tipos',
		'tnoconformidad',
		'tipo_gastos',
		#No hacen falta
		'asientos_tipo',
		'historia',
		'calendarios',
		't-obra',
		#Otras
		'fcartas',
		'id_linea',
		'cocosat_parametros',
		'cocosat_usuario',
		'delegaciones_tipos',
		'partes_generacion_info',
		'partes_cuadro_mando_parametros',
		'presupuestos_revisiones',
		'presupuestos_impresion_modelo',
		'vehiculos_tipos',
	]
	y = open(os.getcwd()+'\dict.txt', 'w')
	for key in data_base.keys():
		if key in dicts_ignore:
			continue
		description, nada, sec_index, code_len = data_base[key][:4]

		y.write(str((key,description, data_base[key][4]))+'\n')
		add = True
		list_relations = list()
		for linea in data_base[key][2]:
			code, name, format_data, columns, relation, formula = linea[:6]

			relations = list()

			if columns > 0:
				y.write(str((key+'/'+code, name))+'\n')
				relations = relation.split('|')
				for i in range(len(relations)-1,-1,-1):
					if '/' in relations[i]:
						relations[i] = relations[i].split('/')[1]
					if relations[i] == '':
						del relations[i]
						continue

					if relations[i] not in list_relations:
						list_relations.append(relations[i])

				if relations == ['']:
					relations = list()
			else:
				if relation:
					if relation.endswith('/s'):
						relation = relation[:-2]
					if '/' in relation:
						raise ValueError(linea)
					if relation not in list_relations:
						list_relations.append(relation)

			if relation or relations:
				add = False

		if add:
			files_lvl_1.append(key)
		elif len(list_relations) < 7:
			files_lvl_2.append(key)
		else:
			files_lvl_3.append(key)
	y.close()
	return files_lvl_1, files_lvl_2, files_lvl_3

def get_models(files, data_base):
	text=str()
	dc_parser = dict()
	ignore = ['SER_OBS', 'CLC_CERT']
	# Se recorren todos los diccionarios
	for key in files:
		aux = key

		# Se parsea el nombre de la tabla
		if '-' in aux:
			aux = key.split('-')
			for i in range(len(aux)):
				if aux[i] == 'alb':
					aux[i] = 'Albaran'
				else:
					aux[i] = aux[i].capitalize()
			table = ''.join(aux)
		else:
			table = aux.capitalize()
		if table == 'Fcli_acs':
			table = 'acciones_comerciales'
		if text:
			text += '\n'

		# Diccionario para relacionar el código del ERP co el nombre de la columna
		dc_parser[table] = dc_parser.get(table, {})


		# Se empieza a construir la clase y se añaden los campos empresa y código por defecto
		text += '\n'
		text += "class %s(models.Model):" % table
		text += '\n'
		text += "    empresa = models.ForeignKey(Empresa, on_delete=models.CASCADE)  # ForeignKey"
		text += '\n'
		text += "    codigo = models.CharField(max_length=50, unique=True)"
		text += '\n'

		tablas_relacionadas = list()
		campos_repetidos = list()
		names_rels = list()
		contador = 1
		to_str = ''
		k=-1

		for linea in data_base[key][2]:
			k += 1
			codigo, _nombre, formato, columnas, relacion, formula = linea[:6]

			if _nombre == 'denominacion':
				nombre = 'nombre'
			else:
				nombre = sanitize(_nombre)

			if formula:
				continue
			if columnas > 0 and codigo not in ignore:
				tablas_relacionadas.append(linea)
				continue


			# Se comprueba que no existe otra columna con el mismo nombre
			if nombre in campos_repetidos:
				nombre += str(contador)
				contador += 1
			campos_repetidos.append(nombre)

			# Se omiten los compas libres ya que no se utilizan en el ERP
			if nombre.startswith('libre'):
				continue

			# Relación campo ERP con nombre db
			dc_parser[table][k] = (nombre, formato, relacion)

			# Se añade la función __str__
			if not to_str:
				to_str = "\n    def __str__(self):"
				to_str += "\n        return f'Empresa: {self.empresa}: self.empresa.id - Código: {self.codigo}"
				if 'deno' in nombre or 'nombre' in nombre:
					to_str += " - Nombre: {self.%s}'" % nombre
				else:
					to_str +="'"

			# Se parse el tipo de dato del ERP
			if formato in ['%', 'l']:
				fmt = " = models.CharField(max_length=100, default='', null=True)"
			elif formato in ['d']:
				fmt = " = models.DateField()"
			elif formato in ['i']:
				fmt = " = models.IntegerField(default=0, null=True)"
			else:
				if not formato:
					formato = '5'
				fmt = " = models.DecimalField(max_digits=15, decimal_places=%s, default=0, null=True)" % formato

			text += "    " + nombre + fmt
			text += "\n"

		# Se añade la función __str__ al resultado

		if to_str:
			text += to_str

		# Se crean las tablas relacionadas
		for linea in tablas_relacionadas:
			codigo, nombre, formato, columnas, relacion, formula = linea[:6]
			table_rel = table + codigo.split("_")[1].lower().capitalize()

			if relacion:
				has_relations = True

			dc_parser[table_rel] = dc_parser.get(table_rel, {})
			names_rels.append(table_rel)

			text += '\n'
			text += '\n'
			text += "class %s(models.Model):" % table_rel
			text += '\n'
			text += "    empresa = models.ForeignKey(Empresa, on_delete=models.CASCADE)  # ForeignKey"
			text += '\n'
			text += "    %s = models.ForeignKey(%s, on_delete=models.CASCADE)  # ForeignKey" % (table.lower(), table)
			text += '\n'
			relacion = relacion.split("|")
			dc_relacion = {}
			for rel in relacion:
				if not rel:
					continue
				try:
					col, rel = rel.split('/')[:2]
				except:
					raise ValueError(rel)
				dc_relacion[int(col)] = rel
			campos = nombre.split("|")
			formatos = formato.split(' ')
			contador_rel = 1
			campos_repetidos_relacionados = []
			for i in range(len(campos)):
				try:
					fmt = formatos[i]
				except:
					continue
				try:
					rel = relacion[i]
				except:
					rel=''
				if fmt in ['%', 'l']:
					formato = " = models.CharField(max_length=100, default='', null=True)"
				elif fmt in ['d']:
					formato = " = models.DateField()"
				elif fmt in ['i']:
					formato = " = models.IntegerField(default=0, null=True)"
				else:
					formato = " = models.DecimalField(max_digits=15, decimal_places=%s, default=0, null=True)" % fmt
				name = sanitize(campos[i])

				if name in campos_repetidos_relacionados:
					name += str(contador_rel)
					contador_rel += 1
				campos_repetidos_relacionados.append(name)

				if name == 'libre':
					continue
				text += "    " + name + formato
				text += "\n"
				dc_parser[table_rel][i] = (name, fmt, dc_relacion.get(i, ''))


	return text, dc_parser

if __name__ == '__main__':

	ruta = "D:/Server/Applications/tesein/Dcts"
	dicts = bkopen(ruta, huf=1)


	result_level_1, result_level_2, result_level_3 = get_level(dicts)

	'''txt, dc = get_models(result_level_1, dicts)
	path_models = os.path.normpath(os.getcwd()+'\models_n1.py')
	open(path_models, 'w').write(txt)
	path_dc = os.path.normpath(os.getcwd()+'\dc_n1.json')
	open(path_dc, 'w').write(json.dumps(dc, indent=4, sort_keys=True))'''
	ls = [
		'actividades',
		'administradores',
		'almacenes',
		'bancos',
		'capitulos',
		'categorias',
		'fpago',
		'grupos',
		'normativa',
		'normativa_capitulos',
		'nseries_caracteristica',
		'nseries_fechas',
		'paises',
		'postal',
		'subfamilias',
		'subsectores',
		'subzonas',
		'transportistas'
	]
	for item in ls:
		result_level_2.remove(item)

	#txt_ls, dc_ls = get_models(ls, dicts)
	#path_models_ls = os.path.normpath(os.getcwd()+'\models_ls.py')
	#open(path_models_ls, 'w').write(txt_ls)
	#path_dc_ls = os.path.normpath(os.getcwd()+'\dc_ls.json')
	#open(path_dc_ls, 'w').write(json.dumps(dc_ls, indent=4, sort_keys=True))

	'''txt2, dc2 = get_models(result_level_2, dicts)
	path_models2 = os.path.normpath(os.getcwd()+'\models_n2.py')
	open(path_models2, 'w').write(txt2)
	path_dc2 = os.path.normpath(os.getcwd()+'\dc_n2.json')
	open(path_dc2, 'w').write(json.dumps(dc2, indent=4, sort_keys=True))'''

	'''txt3, dc3 = get_models(result_level_3, dicts)
	path_models3 = os.path.normpath(os.getcwd()+'\models_n3.py')
	open(path_models3, 'w').write(txt3)
	path_dc3 = os.path.normpath(os.getcwd()+'\dc_n3.json')
	open(path_dc3, 'w').write(json.dumps(dc3, indent=4, sort_keys=True))'''

	'''tipos = [
		# Tipos
		'llamadas_tipos',
		'acciones_tipos',
		'contratos_clases',
		'contratos_ss',
		'llamadas_tipos',
		'obras_tipos',
		'partes_prioridades',
		'partes_tipos',
		'tnoconformidad',
		'tipo_gastos'
	]
	txt_tipos, dc_tipos = get_models(tipos, dicts)

	path_models_tipos = os.path.normpath(os.getcwd()+'\models_tipos.py')
	open(path_models_tipos, 'w').write(txt_tipos)
	path_dc_tipos = os.path.normpath(os.getcwd()+'\dc_tipos.json')
	open(path_dc_tipos, 'w').write(json.dumps(dc_tipos, indent=4, sort_keys=True))'''

	#tt = len(result_level_1) + len(result_level_2) + len(result_level_3)
	print('\n'.join(result_level_3))
	txt, dc = get_models(result_level_3, dicts)
	path_models = os.path.normpath(os.getcwd() + '\models_n3.py')
	#open(path_models, 'w').write(txt)
	path_dc = os.path.normpath(os.getcwd() + '\dc_n3.json')
	#open(path_dc, 'w').write(json.dumps(dc, indent=4, sort_keys=True))

	txt_ls, dc_ls = get_models(['inventario'], dicts)
	print(txt_ls)