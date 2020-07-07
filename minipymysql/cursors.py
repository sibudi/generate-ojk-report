from __future__ import print_function,absolute_import
from functools import partial
import re
import warnings
from._compat import range_type,text_type,PY2
from.import err
RE_INSERT_VALUES=re.compile(r"\s*((?:INSERT|REPLACE)\b.+\bVALUES?\s*)"+r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))"+r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",re.IGNORECASE|re.DOTALL)
class Cursor(object):
 max_stmt_length=1024000
 _defer_warnings=False
 def __init__(self,connection):
  self.connection=connection
  self.description=None
  self.rownumber=0
  self.rowcount=-1
  self.arraysize=1
  self._executed=None
  self._result=None
  self._rows=None
  self._warnings_handled=False
 def close(self):
  conn=self.connection
  if conn is None:
   return
  try:
   while self.nextset():
    pass
  finally:
   self.connection=None
 def __enter__(self):
  return self
 def __exit__(self,*exc_info):
  del exc_info
  self.close()
 def _get_db(self):
  if not self.connection:
   raise err.ProgrammingError("Cursor closed")
  return self.connection
 def _check_executed(self):
  if not self._executed:
   raise err.ProgrammingError("execute() first")
 def _conv_row(self,row):
  return row
 def setinputsizes(self,*args):
  pass
 def setoutputsizes(self,*args):
  pass
 def _nextset(self,unbuffered=False):
  conn=self._get_db()
  current_result=self._result
  if unbuffered:
   self._show_warnings()
  if current_result is None or current_result is not conn._result:
   return None
  if not current_result.has_next:
   return None
  self._result=None
  self._clear_result()
  conn.next_result(unbuffered=unbuffered)
  self._do_get_result()
  return True
 def nextset(self):
  return self._nextset(False)
 def _ensure_bytes(self,x,encoding=None):
  if isinstance(x,text_type):
   x=x.encode(encoding)
  elif isinstance(x,(tuple,list)):
   x=type(x)(self._ensure_bytes(v,encoding=encoding)for v in x)
  return x
 def _escape_args(self,args,conn):
  ensure_bytes=partial(self._ensure_bytes,encoding=conn.encoding)
  if isinstance(args,(tuple,list)):
   if PY2:
    args=tuple(map(ensure_bytes,args))
   return tuple(conn.literal(arg)for arg in args)
  elif isinstance(args,dict):
   if PY2:
    args={ensure_bytes(key):ensure_bytes(val)for(key,val)in args.items()}
   return{key:conn.literal(val)for(key,val)in args.items()}
  else:
   if PY2:
    args=ensure_bytes(args)
   return conn.escape(args)
 def mogrify(self,query,args=None):
  conn=self._get_db()
  if PY2: 
   query=self._ensure_bytes(query,encoding=conn.encoding)
  if args is not None:
   query=query%self._escape_args(args,conn)
  return query
 def execute(self,query,args=None):
  while self.nextset():
   pass
  query=self.mogrify(query,args)
  result=self._query(query)
  self._executed=query
  return result
 def executemany(self,query,args):
  if not args:
   return
  m=RE_INSERT_VALUES.match(query)
  if m:
   q_prefix=m.group(1)%()
   q_values=m.group(2).rstrip()
   q_postfix=m.group(3)or ''
   assert q_values[0]=='(' and q_values[-1]==')'
   return self._do_execute_many(q_prefix,q_values,q_postfix,args,self.max_stmt_length,self._get_db().encoding)
  self.rowcount=sum(self.execute(query,arg)for arg in args)
  return self.rowcount
 def _do_execute_many(self,prefix,values,postfix,args,max_stmt_length,encoding):
  conn=self._get_db()
  escape=self._escape_args
  if isinstance(prefix,text_type):
   prefix=prefix.encode(encoding)
  if PY2 and isinstance(values,text_type):
   values=values.encode(encoding)
  if isinstance(postfix,text_type):
   postfix=postfix.encode(encoding)
  sql=bytearray(prefix)
  args=iter(args)
  v=values%escape(next(args),conn)
  if isinstance(v,text_type):
   if PY2:
    v=v.encode(encoding)
   else:
    v=v.encode(encoding,'surrogateescape')
  sql+=v
  rows=0
  for arg in args:
   v=values%escape(arg,conn)
   if isinstance(v,text_type):
    if PY2:
     v=v.encode(encoding)
    else:
     v=v.encode(encoding,'surrogateescape')
   if len(sql)+len(v)+len(postfix)+1>max_stmt_length:
    rows+=self.execute(sql+postfix)
    sql=bytearray(prefix)
   else:
    sql+=b','
   sql+=v
  rows+=self.execute(sql+postfix)
  self.rowcount=rows
  return rows
 def callproc(self,procname,args=()):
  conn=self._get_db()
  if args:
   fmt='@_{0}_%d=%s'.format(procname)
   self._query('SET %s'%','.join(fmt%(index,conn.escape(arg))for index,arg in enumerate(args)))
   self.nextset()
  q="CALL %s(%s)"%(procname,','.join(['@_%s_%d'%(procname,i)for i in range_type(len(args))]))
  self._query(q)
  self._executed=q
  return args
 def fetchone(self):
  self._check_executed()
  if self._rows is None or self.rownumber>=len(self._rows):
   return None
  result=self._rows[self.rownumber]
  self.rownumber+=1
  return result
 def fetchmany(self,size=None):
  self._check_executed()
  if self._rows is None:
   return()
  end=self.rownumber+(size or self.arraysize)
  result=self._rows[self.rownumber:end]
  self.rownumber=min(end,len(self._rows))
  return result
 def fetchall(self):
  self._check_executed()
  if self._rows is None:
   return()
  if self.rownumber:
   result=self._rows[self.rownumber:]
  else:
   result=self._rows
  self.rownumber=len(self._rows)
  return result
 def scroll(self,value,mode='relative'):
  self._check_executed()
  if mode=='relative':
   r=self.rownumber+value
  elif mode=='absolute':
   r=value
  else:
   raise err.ProgrammingError("unknown scroll mode %s"%mode)
  if not(0<=r<len(self._rows)):
   raise IndexError("out of range")
  self.rownumber=r
 def _query(self,q):
  conn=self._get_db()
  self._last_executed=q
  self._clear_result()
  conn.query(q)
  self._do_get_result()
  return self.rowcount
 def _clear_result(self):
  self.rownumber=0
  self._result=None
  self.rowcount=0
  self.description=None
  self.lastrowid=None
  self._rows=None
 def _do_get_result(self):
  conn=self._get_db()
  self._result=result=conn._result
  self.rowcount=result.affected_rows
  self.description=result.description
  self.lastrowid=result.insert_id
  self._rows=result.rows
  self._warnings_handled=False
  if not self._defer_warnings:
   self._show_warnings()
 def _show_warnings(self):
  if self._warnings_handled:
   return
  self._warnings_handled=True
  if self._result and(self._result.has_next or not self._result.warning_count):
   return
  ws=self._get_db().show_warnings()
  if ws is None:
   return
  for w in ws:
   msg=w[-1]
   if PY2:
    if isinstance(msg,unicode):
     msg=msg.encode('utf-8','replace')
   warnings.warn(err.Warning(*w[1:3]),stacklevel=4)
 def __iter__(self):
  return iter(self.fetchone,None)
 Warning=err.Warning
 Error=err.Error
 InterfaceError=err.InterfaceError
 DatabaseError=err.DatabaseError
 DataError=err.DataError
 OperationalError=err.OperationalError
 IntegrityError=err.IntegrityError
 InternalError=err.InternalError
 ProgrammingError=err.ProgrammingError
 NotSupportedError=err.NotSupportedError
class DictCursorMixin(object):
 dict_type=dict
 def _do_get_result(self):
  super(DictCursorMixin,self)._do_get_result()
  fields=[]
  if self.description:
   for f in self._result.fields:
    name=f.name
    if name in fields:
     name=f.table_name+'.'+name
    fields.append(name)
   self._fields=fields
  if fields and self._rows:
   self._rows=[self._conv_row(r)for r in self._rows]
 def _conv_row(self,row):
  if row is None:
   return None
  return self.dict_type(zip(self._fields,row))
class DictCursor(DictCursorMixin,Cursor):
 pass
class SSCursor(Cursor):
 _defer_warnings=True
 def _conv_row(self,row):
  return row
 def close(self):
  conn=self.connection
  if conn is None:
   return
  if self._result is not None and self._result is conn._result:
   self._result._finish_unbuffered_query()
  try:
   while self.nextset():
    pass
  finally:
   self.connection=None
 __del__=close
 def _query(self,q):
  conn=self._get_db()
  self._last_executed=q
  self._clear_result()
  conn.query(q,unbuffered=True)
  self._do_get_result()
  return self.rowcount
 def nextset(self):
  return self._nextset(unbuffered=True)
 def read_next(self):
  return self._conv_row(self._result._read_rowdata_packet_unbuffered())
 def fetchone(self):
  self._check_executed()
  row=self.read_next()
  if row is None:
   self._show_warnings()
   return None
  self.rownumber+=1
  return row
 def fetchall(self):
  return list(self.fetchall_unbuffered())
 def fetchall_unbuffered(self):
  return iter(self.fetchone,None)
 def __iter__(self):
  return self.fetchall_unbuffered()
 def fetchmany(self,size=None):
  self._check_executed()
  if size is None:
   size=self.arraysize
  rows=[]
  for i in range_type(size):
   row=self.read_next()
   if row is None:
    self._show_warnings()
    break
   rows.append(row)
   self.rownumber+=1
  return rows
 def scroll(self,value,mode='relative'):
  self._check_executed()
  if mode=='relative':
   if value<0:
    raise err.NotSupportedError("Backwards scrolling not supported by this cursor")
   for _ in range_type(value):
    self.read_next()
   self.rownumber+=value
  elif mode=='absolute':
   if value<self.rownumber:
    raise err.NotSupportedError("Backwards scrolling not supported by this cursor")
   end=value-self.rownumber
   for _ in range_type(end):
    self.read_next()
   self.rownumber=value
  else:
   raise err.ProgrammingError("unknown scroll mode %s"%mode)
class SSDictCursor(DictCursorMixin,SSCursor):
 pass
