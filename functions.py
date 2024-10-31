# -*- coding: utf-8 -*-
import bsddb
import os
import re
from StringIO import StringIO
from cPickle import Pickler, Unpickler
from zlib import *
from _bsddb import *
from time import *

MX = chr(30)

def Fecha_aNum(v):
    if v == '':
        return
    else:
        if len(v) < 8:
            d, m, y = v[:2], v[2:4], v[4:]
            dh, mh, yh = Fecha('dmy')
            d = int(d)
            if m != '':
                m = int(m)
            else:
                m = mh
            if y != '':
                y = int(y)
            else:
                y = yh
        else:
            try:
                d, m, y = int(v[:2]), int(v[2:4]), int(v[4:])
            except:
                return

            if Veri_Fecha(d, m, y):
                return d - 1 + (m - 1) * 31 + (y - 2000) * 372
        return


def Fecha(fmt='', t='', fmti=0):
    if t == '':
        t = localtime(time())
    else:
        if t > 2147483600.0:
            t = 2147483600.0
        t = localtime(t)
    if fmt == 't':
        return time()
    d, m, y, hora, minu, sec, dia_s = (
     t[2],
     t[1],
     t[0],
     t[3],
     t[4],
     t[5],
     t[6])
    if fmt == 'd':
        return d
    if fmt == 'ds':
        return ajus0(str(d), 2)
    if fmt == 'm':
        return m
    if fmt == 'ms':
        return ajus0(str(m), 2)
    if fmt == 'a':
        return y
    if fmt == 'as':
        return ajus0(str(y), 4)
    if fmt == 'hm':
        return ajus0(str(hora), 2) + ':' + ajus0(str(minu), 2)
    if fmt == 'hms':
        return ajus0(str(hora), 2) + ':' + ajus0(str(minu), 2) + ':' + ajus0(str(sec), 2)
    if fmt in ('X', 'x'):
        da = ajus0(str(d), 2)
        ma = ajus0(str(m), 2)
        ya = ajus0(str(y), 4)
        sep = '/'
        if fmt == 'x':
            sep = ''
        if fmti == 0:
            return da + sep + ma + sep + ya
        if fmti == 1:
            return ma + sep + da + sep + ya
        return ya + sep + ma + sep + da
    else:
        if fmt == 'dmy':
            return (d,
             m,
             y)
        if fmt in ('1e', 'fa', '1m'):
            ya = ajus0(str(y), 4)
            da = '01'
            if fmt == 'fa':
                da = '31'
            ma = ajus0(str(m), 2)
            if fmt == '1e':
                ma = '01'
            if fmt == 'fa':
                ma = '12'
            return Fecha_aNum(da + ma + ya)
        if fmt == 'dsm':
            return dia_s
    da = ajus0(str(d), 2)
    ma = ajus0(str(m), 2)
    ya = ajus0(str(y), 4)
    return Fecha_aNum(da + ma + ya)

def Veri_Fecha(d, m, y):
    malo = 1
    if m < 1 or m > 12 or d > 31 or d < 1:
        malo = 0
    if m in (4, 6, 9, 11) and d > 30:
        malo = 0
    if y < 1 or y > 9999:
        malo = 0
    if m == 2:
        if (y / 4.0 != int(y / 4.0) or y / 100.0 == int(y / 100.0) and y / 400.0 != int(y / 400.0)) and d > 28:
            malo = 0
        if d > 29:
            malo = 0
    return malo

def Num_aFecha(n, fmt='', fmti=0):
    if (not n) == 1 and n != 0:
        return None
    else:
        n = int(n)
        y = int(n / 372)
        v = n - 372 * y
        m = int(v / 31)
        d = v - 31 * m + 1
        m = m + 1
        y = y + 2000
        if Veri_Fecha(d, m, y) == 0:
            return
        if fmt in ('', 'x', 'x2'):
            da = ajus0(str(d), 2)
            ma = ajus0(str(m), 2)
            ya = ajus0(str(y), 4)
            sep = '/'
            if fmt in ('x', 'x2'):
                sep = ''
            if fmt == 'x2':
                ya = ya[2:]
            if fmti == 0:
                return da + sep + ma + sep + ya
            if fmti == 1:
                return ma + sep + da + sep + ya
            return ya + sep + ma + sep + da
        else:
            if fmt == 'd':
                return d
            if fmt == 'ds':
                return ajus0(str(d), 2)
            if fmt == 'm':
                return m
            if fmt == 'm-':
                return m - 1
            if fmt == 'ms':
                return ajus0(str(m), 2)
            if fmt == 'dmy':
                return (d,
                 m,
                 y)
            if fmt == 'a':
                return y
            if fmt == 'as':
                return ajus0(str(y), 4)
            if fmt == 'dsm':
                f1 = mktime((y,
                 m,
                 d,
                 2,
                 0,
                 0,
                 0,
                 0,
                 -1))
                return Fecha('dsm', f1)
        return

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


def bkopen(archivo, forma='c', tipo='b', borrar='', inx=(), cache=1, huf=0, tmk=0):
    fm = DB_CREATE
    if borrar == 's':
        fm = fm | DB_TRUNCATE
    if forma == 'r':
        fm = DB_RDONLY
    tp = DB_BTREE
    if tipo == 'h':
        tp = DB_HASH
    d = DBcp(archivo, borrar, inx, cache, huf, tmk)
    try:
        d.open(archivo, dbtype=tp, flags=fm | DB_THREAD)
    except bsddb.db.DBNoSuchFileError as e:
        raise ValueError("No existe la tabla "+archivo)
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
        if fi is not None:
            return DBcpCursor(fi.db.cursor(None, 0))
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


def sanitize(text, limit=20):
    if type(text) == str:
        text = text.decode('latin1').encode('utf8')
        text = text.lower()
    else:
        text = str(text)
    text = text.replace('-', '_')
    text = text.replace(',', '_')
    text = text.replace('.', '_')
    text = text.replace(':', '_')
    text = text.replace('&', '_')
    text = text.replace(' ', '_')
    text = text.replace('º', '')
    text = text.replace('(', '')
    text = text.replace(')', '')
    text = text.replace('?', '')
    text = text.replace('¿', '')
    text = text.replace('+', '')
    text = text.replace('*', '')
    text = text.replace('@', 'a')
    text = text.replace('á', 'a')
    text = text.replace('à', 'a')
    text = text.replace('é', 'e')
    text = text.replace('í', 'i')
    text = text.replace('ì', 'i')
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
    text = text.replace('____', '_')
    text = text.replace('___', '_')
    text = text.replace('__', '_')

    text = text[:limit]

    if text.endswith('_'):
        text = text[:-1]
    if text=='id':
        text='id_registro'

    if not text:
        text = 'vacio'

    if text[0].isdigit():
        text = text[1:]

    if text.startswith('_'):
        text = text[1:]

    return text


def stripRtf(text, res='plain'):
    if type(text) == list:
        return '\n'.join(text)
    if not text.startswith('{\\rtf1'):
        return text

    pattern = re.compile(r"\\([a-z]{1,32})(-?\d{1,10})?[ ]?|\\'([0-9a-f]{2})|\\([^a-z])|([{}])|[\r\n]+|(.)", re.I)
    # control words which specify a "destionation".
    destinations = frozenset((
        'aftncn', 'aftnsep', 'aftnsepc', 'annotation', 'atnauthor', 'atndate', 'atnicn', 'atnid', 'atnparent', 'atnref',
        'atntime', 'atrfend', 'atrfstart', 'author', 'background',
        'bkmkend', 'bkmkstart', 'blipuid', 'buptim', 'category', 'colorschememapping', 'colortbl', 'comment', 'company',
        'creatim', 'datafield', 'datastore', 'defchp', 'defpap',
        'do', 'doccomm', 'docvar', 'dptxbxtext', 'ebcend', 'ebcstart', 'factoidname', 'falt', 'fchars', 'ffdeftext',
        'ffentrymcr', 'ffexitmcr', 'ffformat', 'ffhelptext', 'ffl',
        'ffname', 'ffstattext', 'field', 'file', 'filetbl', 'fldinst', 'fldrslt', 'fldtype', 'fname', 'fontemb',
        'fontfile', 'fonttbl', 'footer', 'footerf', 'footerl', 'footerr',
        'footnote', 'formfield', 'ftncn', 'ftnsep', 'ftnsepc', 'g', 'generator', 'gridtbl', 'header', 'headerf',
        'headerl', 'headerr', 'hl', 'hlfr', 'hlinkbase', 'hlloc', 'hlsrc',
        'hsv', 'htmltag', 'info', 'keycode', 'keywords', 'latentstyles', 'lchars', 'levelnumbers', 'leveltext',
        'lfolevel', 'linkval', 'list', 'listlevel', 'listname', 'listoverride',
        'listoverridetable', 'listpicture', 'liststylename', 'listtable', 'listtext', 'lsdlockedexcept', 'macc',
        'maccPr', 'mailmerge', 'maln', 'malnScr', 'manager', 'margPr',
        'mbar', 'mbarPr', 'mbaseJc', 'mbegChr', 'mborderBox', 'mborderBoxPr', 'mbox', 'mboxPr', 'mchr', 'mcount',
        'mctrlPr', 'md', 'mdeg', 'mdegHide', 'mden', 'mdiff', 'mdPr', 'me',
        'mendChr', 'meqArr', 'meqArrPr', 'mf', 'mfName', 'mfPr', 'mfunc', 'mfuncPr', 'mgroupChr', 'mgroupChrPr',
        'mgrow', 'mhideBot', 'mhideLeft', 'mhideRight', 'mhideTop', 'mhtmltag',
        'mlim', 'mlimloc', 'mlimlow', 'mlimlowPr', 'mlimupp', 'mlimuppPr', 'mm', 'mmaddfieldname', 'mmath', 'mmathPict',
        'mmathPr', 'mmaxdist', 'mmc', 'mmcJc', 'mmconnectstr',
        'mmconnectstrdata', 'mmcPr', 'mmcs', 'mmdatasource', 'mmheadersource', 'mmmailsubject', 'mmodso',
        'mmodsofilter', 'mmodsofldmpdata', 'mmodsomappedname', 'mmodsoname',
        'mmodsorecipdata', 'mmodsosort', 'mmodsosrc', 'mmodsotable', 'mmodsoudl', 'mmodsoudldata', 'mmodsouniquetag',
        'mmPr', 'mmquery', 'mmr', 'mnary', 'mnaryPr',
        'mnoBreak', 'mnum', 'mobjDist', 'moMath', 'moMathPara', 'moMathParaPr', 'mopEmu', 'mphant', 'mphantPr',
        'mplcHide', 'mpos', 'mr', 'mrad', 'mradPr', 'mrPr', 'msepChr',
        'mshow', 'mshp', 'msPre', 'msPrePr', 'msSub', 'msSubPr', 'msSubSup', 'msSubSupPr', 'msSup', 'msSupPr',
        'mstrikeBLTR', 'mstrikeH', 'mstrikeTLBR', 'mstrikeV', 'msub', 'msubHide',
        'msup', 'msupHide', 'mtransp', 'mtype', 'mvertJc', 'mvfmf', 'mvfml', 'mvtof', 'mvtol', 'mzeroAsc', 'mzeroDesc',
        'mzeroWid', 'nesttableprops', 'nextfile', 'nonesttables',
        'objalias', 'objclass', 'objdata', 'object', 'objname', 'objsect', 'objtime', 'oldcprops', 'oldpprops',
        'oldsprops', 'oldtprops', 'oleclsid', 'operator', 'panose', 'password',
        'passwordhash', 'pgp', 'pgptbl', 'picprop', 'pict', 'pn', 'pnseclvl', 'pntext', 'pntxta', 'pntxtb', 'printim',
        'private', 'propname', 'protend', 'protstart', 'protusertbl', 'pxe',
        'result', 'revtbl', 'revtim', 'rsidtbl', 'rxe', 'shp', 'shpgrp', 'shpinst', 'shppict', 'shprslt', 'shptxt',
        'sn', 'sp', 'staticval', 'stylesheet', 'subject', 'sv',
        'svb', 'tc', 'template', 'themedata', 'title', 'txe', 'ud', 'upr', 'userprops', 'wgrffmtfilter',
        'windowcaption', 'writereservation', 'writereservhash', 'xe', 'xform',
        'xmlattrname', 'xmlattrvalue', 'xmlclose', 'xmlname', 'xmlnstbl', 'xmlopen'
    ))
    # Translation of some special characters.
    specialchars = {
        'par': '\n', 'sect': '\n\n', 'page': '\n\n', 'line': '\n', 'tab': '\t',
        'emdash': u'\u2014',
        'endash': u'\u2013',
        'emspace': u'\u2003',
        'enspace': u'\u2002',
        'qmspace': u'\u2005',
        'bullet': u'\u2022',
        'lquote': u'\u2018',
        'rquote': u'\u2019',
        'ldblquote': u'\201C',
        'rdblquote': u'\u201D',
    }

    stack = []
    ignorable = False  # Whether this group (and all inside it) are "ignorable".
    ucskip = 1  # Number of ASCII characters to skip after a unicode character.
    curskip = 0  # Number of ASCII characters left to skip
    # out = []                     # Output buffer.
    c = 0

    dc = {}
    new_bloq = False  ##Indicara si empieza un nuevo bloque
    id_bloq = -1
    id_element = 0
    bloque = ''
    key = ''
    end_brace = False

    for match in pattern.finditer(text):
        c += 1
        # if c>143:break
        etiqueta, arg, hexadecimal, char, brace, letra = match.groups()
        # print etiqueta,arg,hexadecimal,char,brace,letra
        if brace:
            # print c,'brace', brace
            curskip = 0
            if brace == '{' and bloque not in ['body', 'fonttbl']:
                new_bloq = True
                id_bloq += 1
                id_element = 0
                if bloque == 'stylesheet':
                    end_brace = False
                # Push state
                stack.append((ucskip, ignorable))
            elif brace == '}' and bloque not in ['body', 'fonttbl']:
                if bloque == 'stylesheet':
                    if end_brace:
                        new_bloq = True
                        id_bloq += 1
                        id_element = 0
                        bloque = 'properties'
                        end_brace = False
                    else:
                        end_brace = True

                ucskip, ignorable = stack.pop()

        elif char:  # \x (not a letter)
            # print c,'char', char
            curskip = 0
            if char == '~':
                if not ignorable:
                    # out.append(u'\xA0')
                    dc[id_bloq, bloque][key].append(u'\xA0')
            elif char in '{}\\':
                if not ignorable:
                    # out.append(char)
                    dc[id_bloq, bloque][key].append(char.encode('latin-1'))
            elif char == '*':
                ignorable = True

        elif etiqueta:  # Etiquetas
            # print c,'  ',etiqueta,'-',bloque
            if etiqueta == 'colortbl' or (etiqueta == 'plain' and bloque == 'properties'):
                new_bloq = True
                id_bloq += 1
                id_element = 0
            if new_bloq:
                if etiqueta == 'rtf':
                    bloque = 'rtf'
                elif etiqueta == 'fonttbl':
                    bloque = 'fonttbl'
                elif etiqueta == 'colortbl':
                    bloque = 'colortbl'
                elif etiqueta == 'stylesheet':
                    bloque = 'stylesheet'
                elif etiqueta == 'plain':
                    bloque = 'body'
                # else:bloque='body'
                dc[id_bloq, bloque] = {}
                new_bloq = False
            key = (id_element, etiqueta, arg)
            if key not in dc[id_bloq, bloque].keys():
                dc[id_bloq, bloque][key] = []
            id_element += 1

            # print c,id_bloq,'etiqueta',etiqueta,arg
            curskip = 0
            if etiqueta in destinations:
                ignorable = True
            elif ignorable:
                pass
            elif etiqueta in specialchars:
                # out.append(specialchars[etiqueta])
                dc[id_bloq, bloque][key].append(specialchars[etiqueta])
            elif etiqueta == 'uc':
                ucskip = int(arg)
            elif etiqueta == 'u':
                c = int(arg)
                if c < 0: c += 0x10000
                if c > 127:
                    dc[id_bloq, bloque][key].append('')
                # out.append(unichr(c))
                else:
                    dc[id_bloq, bloque][key].append(chr(c))
                # out.append(chr(c))
                curskip = ucskip

        elif hexadecimal:  # \'xx
            # print c,'hexadecimal', hexadecimal
            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                c = int(hexadecimal, 16)
                if c > 127:
                    dc[id_bloq, bloque][key].append('')
                # out.append(unichr(c))
                else:
                    dc[id_bloq, bloque][key].append(chr(c))
                # out.append(chr(c))
        elif letra:
            ignorable = False

            if curskip > 0:
                curskip -= 1
            elif not ignorable:
                # out.append(letra)

                dc[id_bloq, bloque][key].append(letra)

    ls = list(dc.keys())
    ls.sort()
    text = ''
    for id_bloq, bloque in ls:
        if res == 'plain' and bloque != 'body': continue
        # print id_bloq,bloque
        rs = list(dc[id_bloq, bloque].keys())
        rs.sort()
        for id_element, etiqueta, arg in rs:
            # print '   ',id_element,etiqueta,arg,''.join(dc[id_bloq,bloque][id_element,etiqueta,arg])
            text += ''.join(dc[id_bloq, bloque][id_element, etiqueta, arg])

    if res == 'plain':
        return text#.encode('latin-1')

    else:
        return dc
##
