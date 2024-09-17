# -*- coding: utf-8 -*-

import os
from StringIO import StringIO
from cPickle import Pickler, Unpickler
from zlib import *
from _bsddb import *

MX = chr(30)


def Tx(ms, cl=''):
	if cl == '':
		try:
			mx = ''
			if mx == '':
				return ms
			return mx
		except:
			return ms

	else:
		idioma = cl._idid
		if idioma < 0:
			return ms
		try:
			mx = ''
			if mx == '':
				return ms
			return mx
		except:
			return ms


def lista(cad, sep, min=0, nms='', mx=''):
	ax = cad.split(sep)
	if min > 0:
		while len(ax) < min:
			ax.append('')

	if nms != '':
		l = len(ax)
		if nms == '*':
			bx, k = [], 0
			while len(bx) < l:
				bx.append(str(k))
				k = k + 1

		else:
			bx = nms.split('-')
		for k in bx:
			entero = 1
			if k.find('.') >= 0:
				entero = 0
				k = k.replace('.', '')
			k = int(k)
			if ax[k] == '':
				ax[k] = '0'
			if entero == 0:
				try:
					ax[k] = float(ax[k])
				except:
					ax[k] = 0.0

			else:
				try:
					ax[k] = int(ax[k])
				except:
					ax[k] = 0

	if mx != '':
		return ax[:mx]
	return ax


def Trae_Fila(tabla, valor, clr=0, clb=0, match_ini=''):
	lmax = len(tabla)
	if match_ini != '':
		ldt = len(valor)
	vc = valor
	for k in range(lmax):
		if match_ini != '':
			ldt = len(tabla[k][clb])
			vc = valor[:ldt]
		if tabla[k][clb] == vc:
			if clr == -1:
				return tabla[k]
			if clr == -2:
				return k
			return tabla[k][clr]

	return


def ajus0(s, l):
	res = s.rjust(l).replace(' ', '0')
	la = len(res)
	if la > l:
		return res[la - l:]
	return res


def Palabras_k(v):
	v = v.replace('.', ' ')
	v = v.replace(',', ' ')
	vls = v.split()
	res = []
	for v in vls:
		if len(v) < 2:
			continue
		res.append(v.upper())

	return res


def dump(obj, file, protocol=None):
	Pickler(file, protocol).dump(obj)


def dumps(obj, protocol=None):
	file = StringIO()
	Pickler(file, protocol).dump(obj)
	return file.getvalue()


def load(file):
	return Unpickler(file).load()


def loads(text):
	file = StringIO(text)
	return Unpickler(file).load()


def bkopen(archivo, forma='c', tipo='b', borrar='', inx=[], cache=1, huf=0, tmk=0):
	fm = DB_CREATE
	if borrar == 's':
		fm = fm | DB_TRUNCATE
	if forma == 'r':
		fm = DB_RDONLY
	tp = DB_BTREE
	if tipo == 'h':
		tp = DB_HASH
	d = DBcp(archivo, borrar, inx, cache, huf, tmk)
	d.open(archivo, dbtype=tp, flags=fm | DB_THREAD)
	return d


class DBcp():
	__module__ = __name__

	def __init__(self, archivo, borrar, inx, cache, huf, tmk):
		self.db = DB(None)
		try:
			if tmk != 0:
				tama = tmk
			else:
				tama = os.path.getsize(archivo)
			if tama < 20480:
				chd = 20480
			else:
				if tama < 32768:
					chd = 24576
				else:
					if tama < 65536:
						chd = 45056
					else:
						if tama < 131072:
							chd = 102400
						else:
							if tama < 262144:
								chd = 225280
							else:
								if tama < 524288:
									chd = 0
								else:
									if tama < 921600:
										chd = 307200
									else:
										if tama < 2560000:
											chd = 358400
										else:
											if tama < 5120000:
												chd = 409600
											else:
												chd = 460800
			if tama < 0:
				chd = -tama
			if chd > 0:
				self.db.set_cachesize(0, chd, 0)
		except:
			self.db.set_cachesize(0, 20480, 0)

		self.vx = {}
		self.tx = {}
		self.ch = cache
		self.inx = []
		self.huf = huf
		self.snc = ''
		self._blq = ''
		if inx != []:
			for ncam, cp, col, tp in inx:
				ncam = ncam.lower()
				ls = [cp,
					  col,
					  '',
					  ncam,
					  tp]
				ls[2] = bkopen(archivo + '.' + ncam, borrar=borrar, cache=cache, tmk=tmk)
				self.inx.append(ls)

		return

	def __del__(self):
		self.close()

	def __getattr__(self, name):
		return getattr(self.db, name)

	def __len__(self):
		if self.snc != '':
			return self.snc.__len__()
		return len(self.db)

	def __getitem__(self, idx):
		if self.snc != '':
			return self.snc.__getitem__(idx)
		if self.ch == 0:
			if self.huf == 0:
				return loads(self.db[idx])
			return loads(decompress(self.db[idx]))
		try:
			return self.vx[idx]
		except:
			pass

		if self.huf == 0:
			dato = loads(self.db[idx])
		else:
			dato = loads(decompress(self.db[idx]))
		self.vx[idx] = dato
		return dato

	def __setitem__(self, idx, valor):
		if self.snc != '':
			return self.snc.__setitem__(idx, valor)
		if self.inx != []:
			try:
				valor_a = self[idx]
			except:
				valor_a = []

		if self.huf == 0:
			dato = dumps(valor, -1)
		else:
			dato = compress(dumps(valor, -1))
		self.db[idx] = dato
		if self.ch == 1:
			try:
				self.vx[idx] = valor
			except:
				pass

		if self.inx != []:
			for cps, cols, fi, ncam, tp in self.inx:
				if valor_a != []:
					for k in range(len(cps)):
						cp = cps[k]
						col = cols[k]
						va = valor_a[cp]
						vn = valor[cp]
						if va == vn:
							continue
						if type(va) == list:
							for ln in va:
								if type(ln) == list:
									if Trae_Fila(vn, ln[col], clr=-2, clb=col) != None:
										continue
									v = str(ln[col])
								else:
									if ln in vn:
										continue
									v = str(ln)
								if v == '':
									continue
								if tp == 'd':
									v = ajus0(v, 5)
									if v == '0None':
										v = '-9999'
								if tp == 'i':
									vls = Palabras_k(v)
									for v in vls:
										del fi[v + MX + idx]

								else:
									del fi[v + MX + idx]

						else:
							va = str(va)
							if va == '':
								continue
							if tp == 'd':
								va = ajus0(va, 5)
								if va == '0None':
									va = '-9999'
							if tp == 'i':
								vls = Palabras_k(va)
								for v in vls:
									del fi[v + MX + idx]

							else:
								del fi[va + MX + idx]

				for k in range(len(cps)):
					cp = cps[k]
					col = cols[k]
					vn = valor[cp]
					va = ''
					if valor_a != []:
						va = valor_a[cp]
						if vn == va:
							continue
					if type(vn) == list:
						for ln in vn:
							if type(ln) == list:
								if va != '':
									if Trae_Fila(va, ln[col], clr=-2, clb=col) != None:
										continue
								v = str(ln[col])
							else:
								if va != '':
									if ln in va:
										continue
								v = str(ln)
							if v == '':
								continue
							if tp == 'd':
								v = ajus0(v, 5)
								if v == '0None':
									v = '-9999'
							if tp == 'i':
								vls = Palabras_k(v)
								for v in vls:
									fi[v + MX + idx] = ''

							else:
								fi[v + MX + idx] = ''

					else:
						vn = str(vn)
						if vn == '':
							continue
						if tp == 'd':
							vn = ajus0(vn, 5)
							if vn == '0None':
								vn = '-9999'
						if tp == 'i':
							vls = Palabras_k(vn)
							for v in vls:
								fi[v + MX + idx] = ''

						else:
							fi[vn + MX + idx] = ''

		return

	def __delitem__(self, idx):
		if self.snc != '':
			return self.snc.__delitem__(idx)
		if self.inx != []:
			try:
				valor_a = self[idx]
				for cps, cols, fi, ncam, tp in self.inx:
					for k in range(len(cps)):
						cp = cps[k]
						col = cols[k]
						va = valor_a[cp]
						if type(va) == list:
							for ln in va:
								if type(ln) == list:
									v = str(ln[col])
								else:
									v = str(ln)
								if v == '':
									continue
								if tp == 'd':
									v = ajus0(v, 5)
									if v == '0None':
										v = '-9999'
								if tp == 'i':
									vls = Palabras_k(v)
									for v in vls:
										del fi[v + MX + idx]

								else:
									del fi[v + MX + idx]

						else:
							va = str(va)
							if va == '':
								continue
							if tp == 'd':
								va = ajus0(va, 5)
								if va == '0None':
									va = '-9999'
							if tp == 'i':
								vls = Palabras_k(va)
								for v in vls:
									del fi[v + MX + idx]

							else:
								del fi[va + MX + idx]

			except:
				pass

		if self.ch == 1:
			try:
				del self.vx[idx]
			except:
				pass

		try:
			del self.db[idx]
		except:
			pass

	def cursor(self):
		if self.snc != '':
			return self.snc.cursor()
		return DBcpCursor(self.db.cursor(None, 0))

	def i_cursor(self, ncam):
		if self.snc != '':
			return self.snc.i_cursor(ncam)
		fi = Trae_Fila(self.inx, ncam.lower(), clb=3, clr=2)
		if fi != None:
			return DBcpCursor(fi.db.cursor(None, 0))
		return
		return

	def lee_b(self, idx):
		if self.snc != '':
			return self.snc.lee_b(idx)
		return self.db[idx]

	def graba_b(self, idx, valor):
		if self.snc != '':
			return self.snc.graba_b(idx, valor)
		self.db[idx] = valor

	def i_selec(self, ncam, desde, hasta):
		if self.snc != '':
			return self.snc.i_selec(ncam, desde, hasta)
		tp = None
		for lnx in self.inx:
			if lnx[3] == ncam.lower():
				tp = lnx[4]
				break

		if tp == None:
			return
		res = []
		ers = 0
		cursor = self.i_cursor(ncam)
		if cursor == None:
			return
		if tp == 'd':
			if desde == '' or desde == None:
				ds = -9999
			else:
				ds = int(desde)
			desde = ajus0(str(ds), 5)
			if hasta == '' or hasta == None:
				hs = 99999
			else:
				hs = int(hasta)
			hasta = ajus0(str(hasta), 5)
		else:
			desde, hasta = str(desde), str(hasta)
		try:
			ids = cursor.set_range(desde, sl_cod=1)
			cds, idx = lista(ids, MX, 2)
			if cds >= desde and cds <= hasta:
				res.append(idx)
			else:
				ers = 1
		except:
			ers = 1

		while ers == 0:
			try:
				ids = cursor.next(sl_cod=1)
				cds, idx = lista(ids, MX, 2)
				if cds >= desde and cds <= hasta:
					res.append(idx)
				else:
					ers = 1
			except:
				ers = 1

		del cursor
		return res

	def sincro(self):
		if self.snc != '':
			return self.snc.sincro()
		try:
			self.sync()
		except:
			pass

		if self.inx != []:
			for ln in self.inx:
				try:
					ln[2].sync()
				except:
					pass

	def cierra(self):
		del self.vx
		del self.tx
		self.close()
		if self.inx != []:
			for ln in self.inx:
				ln[2].close()


class DBcpCursor():
	__module__ = __name__

	def __init__(self, cursor):
		self.dbc = cursor

	def __del__(self):
		self.close()

	def __getattr__(self, nombre):
		return getattr(self.dbc, nombre)

	def dup(self, flags=0):
		return DBcpCursor(self.dbc.dup(flags))

	def get_1(self, flags, sl_cod=0):
		rec = self.dbc.get(flags)
		return self._extrae(rec, sl_cod)

	def current(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_CURRENT, sl_cod)

	def first(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_FIRST, sl_cod)

	def last(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_LAST, sl_cod)

	def next(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_NEXT, sl_cod)

	def prev(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_PREV, sl_cod)

	def consume(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_CONSUME, sl_cod)

	def next_dup(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_NEXT_DUP, sl_cod)

	def next_nodup(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_NEXT_NODUP, sl_cod)

	def prev_nodup(self, flags=0, sl_cod=0):
		return self.get_1(flags | DB_PREV_NODUP, sl_cod)

	def set(self, idx, flags=0, sl_cod=0):
		rec = self.dbc.set(idx, flags)
		return self._extrae(rec, sl_cod)

	def set_range(self, idx, flags=0, sl_cod=0):
		rec = self.dbc.set_range(idx, flags)
		return self._extrae(rec, sl_cod)

	def _extrae(self, rec, sl_cod):
		if rec is None:
			raise Tx('No existe clave')
		else:
			if sl_cod == 1:
				return rec[0]
			return (rec[0],
					loads(rec[1]))
		return


def sanitize(text):
	if type(text) == str:
		text = text.decode('latin1').encode('utf8')
		text = text.lower()
	else:
		text = str(text)
	text = text.replace('-', '_')
	text = text.replace(',', '_')
	text = text.replace('.', '_')
	text = text.replace(' ', '_')
	text = text.replace('º', '')
	text = text.replace('?', '')
	text = text.replace('á', 'a')
	text = text.replace('à', 'a')
	text = text.replace('é', 'e')
	text = text.replace('í', 'i')
	text = text.replace('ì', 'i')
	text = text.replace('ò', 'o')
	text = text.replace('ó', 'o')
	text = text.replace('ú', 'u')
	text = text.replace('ù', 'u')
	text = text.replace('ä', 'a')
	text = text.replace('ë', 'e')
	text = text.replace('ï', 'i')
	text = text.replace('ö', 'o')
	text = text.replace('ü', 'u')
	text = text.replace('ñ', 'n')
	text = text.replace('ç', 'c')
	text = text.replace('%', '')
	text = text.replace('€', '')
	text = text.replace('/', '_')
	if text.startswith('_'):
		text = text[1:]
	return text


if __name__ == '__main__':

	ruta = "D:/Server/Companies/jonathan/sat/EJA/articulos"
	art = bkopen(ruta, huf=0)
	# print art['SAA_BOMBA_ELECTRICA']

	ruta = "D:/Server/Applications/rvd/Dcts"
	dicts = bkopen(ruta, huf=1)
	txt = ''
	for key in dicts.keys():
		aux = key
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
		if txt:
			txt += '\n'
		txt += '\n'
		txt += "class %s(models.Model):" % table
		txt += '\n'
		txt += "\tempresa = models.ForeignKey(Empresa, on_delete=models.CASCADE)  # ForeignKey"
		txt += '\n'
		txt += "\tcodigo = models.CharField(max_length=50)"
		txt += '\n'

		tablas_relacionadas = []
		to_str = ''

		for linea in dicts[key][2]:
			codigo, nombre, formato, columnas, relacion, formula = linea[:6]
			if formula:
				continue
			if columnas > 0:
				tablas_relacionadas.append(linea)
				continue

			nombre = sanitize(nombre)

			if not to_str:
				to_str = "\n\tdef __str__(self):"
				to_str += "\n\t\treturn Empresa: {self.empresa}: self.empresa.id - Código: {self.codigo}"
				if 'deno' in nombre or 'nombre' in nombre:
					to_str += " - Nombre: {self.%s}" % nombre

			if formato in ['%', 'l']:
				formato = " = models.CharField(max_length=100, default='')"
			elif formato in ['d']:
				formato = " = models.DateField()"
			elif formato in ['i']:
				formato = " = models.IntegerField(default=0)"
			else:
				formato = " = models.DecimalField(max_digits=15, decimal_places=%s, default=0)" % formato
			txt += "\t" + nombre + formato
			txt += "\n"
		if to_str:
			txt += to_str

		for linea in tablas_relacionadas:
			codigo, nombre, formato, columnas, relacion, formula = linea[:6]
			table_rel = table + codigo.split("_")[1].lower().capitalize()
			txt += '\n'
			txt += '\n'
			txt += "class %s(modelss.Model):" % table_rel
			txt += '\n'
			txt += "\tempresa = models.ForeignKey(Empresa, on_delete=models.CASCADE)  # ForeignKey"
			txt += '\n'
			txt += "\t%s = models.ForeignKey(%s, on_delete=models.CASCADE)  # ForeignKey" % (table.lower(), table)
			txt += '\n'
			campos = nombre.split("|")
			formatos = formato.split(' ')
			for i in range(len(campos)):
				formato = formatos[i]
				if formato in ['%', 'l']:
					formato = " = models.CharField(max_length=100, default='')"
				elif formato in ['d']:
					formato = " = models.DateField()"
				elif formato in ['i']:
					formato = " = models.IntegerField(default=0)"
				else:
					formato = " = models.DecimalField(max_digits=15, decimal_places=%s, default=0)" % formato
				txt += "\t" + sanitize(campos[i]) + formato
				txt += "\n"
	print os.getcwd()
	open('c:/users/jonathan/desktop/models2.py', 'w').write(txt)
