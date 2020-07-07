from __future__ import print_function
from._compat import PY2,range_type,text_type,str_type,JYTHON,IRONPYTHON
import errno
import io
import os
import socket
import struct
import sys
import traceback
import warnings
from.import _auth
from.charset import charset_by_name,charset_by_id
from.constants import CLIENT,COMMAND,CR,FIELD_TYPE,SERVER_STATUS
from.import converters
from.cursors import Cursor
from.optionfile import Parser
from.protocol import(dump_packet,MysqlPacket,FieldDescriptorPacket,OKPacketWrapper,EOFPacketWrapper,LoadLocalPacketWrapper)
from.util import byte2int,int2byte
from.import err,VERSION_STRING
try:
 import ssl
 SSL_ENABLED=True
except ImportError:
 ssl=None
 SSL_ENABLED=False
try:
 import getpass
 DEFAULT_USER=getpass.getuser()
 del getpass
except(ImportError,KeyError):
 DEFAULT_USER=None
DEBUG=False
_py_version=sys.version_info[:2]
if PY2:
 pass
elif _py_version<(3,6):
 _surrogateescape_table=[chr(i)if i<0x80 else chr(i+0xdc00)for i in range(256)]
 def _fast_surrogateescape(s):
  return s.decode('latin1').translate(_surrogateescape_table)
else:
 def _fast_surrogateescape(s):
  return s.decode('ascii','surrogateescape')
if PY2 and not IRONPYTHON:
 from._socketio import SocketIO
 def _makefile(sock,mode):
  return io.BufferedReader(SocketIO(sock,mode))
else:
 def _makefile(sock,mode):
  return sock.makefile(mode)
TEXT_TYPES={FIELD_TYPE.BIT,FIELD_TYPE.BLOB,FIELD_TYPE.LONG_BLOB,FIELD_TYPE.MEDIUM_BLOB,FIELD_TYPE.STRING,FIELD_TYPE.TINY_BLOB,FIELD_TYPE.VAR_STRING,FIELD_TYPE.VARCHAR,FIELD_TYPE.GEOMETRY,}
DEFAULT_CHARSET='utf8mb4'
MAX_PACKET_LEN=2**24-1
def pack_int24(n):
 return struct.pack('<I',n)[:3]
def lenenc_int(i):
 if(i<0):
  raise ValueError("Encoding %d is less than 0 - no representation in LengthEncodedInteger"%i)
 elif(i<0xfb):
  return int2byte(i)
 elif(i<(1<<16)):
  return b'\xfc'+struct.pack('<H',i)
 elif(i<(1<<24)):
  return b'\xfd'+struct.pack('<I',i)[:3]
 elif(i<(1<<64)):
  return b'\xfe'+struct.pack('<Q',i)
 else:
  raise ValueError("Encoding %x is larger than %x - no representation in LengthEncodedInteger"%(i,(1<<64)))
class Connection(object):
 _sock=None
 _auth_plugin_name=''
 _closed=False
 _secure=False
 def __init__(self,host=None,user=None,password="",database=None,port=0,unix_socket=None,charset='',sql_mode=None,read_default_file=None,conv=None,use_unicode=None,client_flag=0,cursorclass=Cursor,init_command=None,connect_timeout=10,ssl=None,read_default_group=None,compress=None,named_pipe=None,autocommit=False,db=None,passwd=None,local_infile=False,max_allowed_packet=16*1024*1024,defer_connect=False,auth_plugin_map=None,read_timeout=None,write_timeout=None,bind_address=None,binary_prefix=False,program_name=None,server_public_key=None):
  if use_unicode is None and sys.version_info[0]>2:
   use_unicode=True
  if db is not None and database is None:
   database=db
  if passwd is not None and not password:
   password=passwd
  if compress or named_pipe:
   raise NotImplementedError("compress and named_pipe arguments are not supported")
  self._local_infile=bool(local_infile)
  if self._local_infile:
   client_flag|=CLIENT.LOCAL_FILES
  if read_default_group and not read_default_file:
   if sys.platform.startswith("win"):
    read_default_file="c:\\my.ini"
   else:
    read_default_file="/etc/my.cnf"
  if read_default_file:
   if not read_default_group:
    read_default_group="client"
   cfg=Parser()
   cfg.read(os.path.expanduser(read_default_file))
   def _config(key,arg):
    if arg:
     return arg
    try:
     return cfg.get(read_default_group,key)
    except Exception:
     return arg
   user=_config("user",user)
   password=_config("password",password)
   host=_config("host",host)
   database=_config("database",database)
   unix_socket=_config("socket",unix_socket)
   port=int(_config("port",port))
   bind_address=_config("bind-address",bind_address)
   charset=_config("default-character-set",charset)
   if not ssl:
    ssl={}
   if isinstance(ssl,dict):
    for key in["ca","capath","cert","key","cipher"]:
     value=_config("ssl-"+key,ssl.get(key))
     if value:
      ssl[key]=value
  self.ssl=False
  if ssl:
   if not SSL_ENABLED:
    raise NotImplementedError("ssl module not found")
   self.ssl=True
   client_flag|=CLIENT.SSL
   self.ctx=self._create_ssl_ctx(ssl)
  self.host=host or "localhost"
  self.port=port or 3306
  self.user=user or DEFAULT_USER
  self.password=password or b""
  if isinstance(self.password,text_type):
   self.password=self.password.encode('latin1')
  self.db=database
  self.unix_socket=unix_socket
  self.bind_address=bind_address
  if not(0<connect_timeout<=31536000):
   raise ValueError("connect_timeout should be >0 and <=31536000")
  self.connect_timeout=connect_timeout or None
  if read_timeout is not None and read_timeout<=0:
   raise ValueError("read_timeout should be >= 0")
  self._read_timeout=read_timeout
  if write_timeout is not None and write_timeout<=0:
   raise ValueError("write_timeout should be >= 0")
  self._write_timeout=write_timeout
  if charset:
   self.charset=charset
   self.use_unicode=True
  else:
   self.charset=DEFAULT_CHARSET
   self.use_unicode=False
  if use_unicode is not None:
   self.use_unicode=use_unicode
  self.encoding=charset_by_name(self.charset).encoding
  client_flag|=CLIENT.CAPABILITIES
  if self.db:
   client_flag|=CLIENT.CONNECT_WITH_DB
  self.client_flag=client_flag
  self.cursorclass=cursorclass
  self._result=None
  self._affected_rows=0
  self.host_info="Not connected"
  self.autocommit_mode=autocommit
  if conv is None:
   conv=converters.conversions
  self.encoders={k:v for(k,v)in conv.items()if type(k)is not int}
  self.decoders={k:v for(k,v)in conv.items()if type(k)is int}
  self.sql_mode=sql_mode
  self.init_command=init_command
  self.max_allowed_packet=max_allowed_packet
  self._auth_plugin_map=auth_plugin_map or{}
  self._binary_prefix=binary_prefix
  self.server_public_key=server_public_key
  self._connect_attrs={'_client_name':'pymysql','_pid':str(os.getpid()),'_client_version':VERSION_STRING,}
  if program_name:
   self._connect_attrs["program_name"]=program_name
  if defer_connect:
   self._sock=None
  else:
   self.connect()
 def _create_ssl_ctx(self,sslp):
  if isinstance(sslp,ssl.SSLContext):
   return sslp
  ca=sslp.get('ca')
  capath=sslp.get('capath')
  hasnoca=ca is None and capath is None
  ctx=ssl.create_default_context(cafile=ca,capath=capath)
  ctx.check_hostname=not hasnoca and sslp.get('check_hostname',True)
  ctx.verify_mode=ssl.CERT_NONE if hasnoca else ssl.CERT_REQUIRED
  if 'cert' in sslp:
   ctx.load_cert_chain(sslp['cert'],keyfile=sslp.get('key'))
  if 'cipher' in sslp:
   ctx.set_ciphers(sslp['cipher'])
  ctx.options|=ssl.OP_NO_SSLv2
  ctx.options|=ssl.OP_NO_SSLv3
  return ctx
 def close(self):
  if self._closed:
   raise err.Error("Already closed")
  self._closed=True
  if self._sock is None:
   return
  send_data=struct.pack('<iB',1,COMMAND.COM_QUIT)
  try:
   self._write_bytes(send_data)
  except Exception:
   pass
  finally:
   self._force_close()
 @property
 def open(self):
  return self._sock is not None
 def _force_close(self):
  if self._sock:
   try:
    self._sock.close()
   except: 
    pass
  self._sock=None
  self._rfile=None
 __del__=_force_close
 def autocommit(self,value):
  self.autocommit_mode=bool(value)
  current=self.get_autocommit()
  if value!=current:
   self._send_autocommit_mode()
 def get_autocommit(self):
  return bool(self.server_status&SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT)
 def _read_ok_packet(self):
  pkt=self._read_packet()
  if not pkt.is_ok_packet():
   raise err.OperationalError(2014,"Command Out of Sync")
  ok=OKPacketWrapper(pkt)
  self.server_status=ok.server_status
  return ok
 def _send_autocommit_mode(self):
  self._execute_command(COMMAND.COM_QUERY,"SET AUTOCOMMIT = %s"%self.escape(self.autocommit_mode))
  self._read_ok_packet()
 def begin(self):
  self._execute_command(COMMAND.COM_QUERY,"BEGIN")
  self._read_ok_packet()
 def commit(self):
  self._execute_command(COMMAND.COM_QUERY,"COMMIT")
  self._read_ok_packet()
 def rollback(self):
  self._execute_command(COMMAND.COM_QUERY,"ROLLBACK")
  self._read_ok_packet()
 def show_warnings(self):
  self._execute_command(COMMAND.COM_QUERY,"SHOW WARNINGS")
  result=MySQLResult(self)
  result.read()
  return result.rows
 def select_db(self,db):
  self._execute_command(COMMAND.COM_INIT_DB,db)
  self._read_ok_packet()
 def escape(self,obj,mapping=None):
  if isinstance(obj,str_type):
   return "'"+self.escape_string(obj)+"'"
  if isinstance(obj,(bytes,bytearray)):
   ret=self._quote_bytes(obj)
   if self._binary_prefix:
    ret="_binary"+ret
   return ret
  return converters.escape_item(obj,self.charset,mapping=mapping)
 def literal(self,obj):
  return self.escape(obj,self.encoders)
 def escape_string(self,s):
  if(self.server_status&SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES):
   return s.replace("'","''")
  return converters.escape_string(s)
 def _quote_bytes(self,s):
  if(self.server_status&SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES):
   return "'%s'"%(_fast_surrogateescape(s.replace(b"'",b"''")),)
  return converters.escape_bytes(s)
 def cursor(self,cursor=None):
  if cursor:
   return cursor(self)
  return self.cursorclass(self)
 def __enter__(self):
  warnings.warn("Context manager API of Connection object is deprecated; Use conn.begin()",DeprecationWarning)
  return self.cursor()
 def __exit__(self,exc,value,traceback):
  if exc:
   self.rollback()
  else:
   self.commit()
 def query(self,sql,unbuffered=False):
  if isinstance(sql,text_type)and not(JYTHON or IRONPYTHON):
   if PY2:
    sql=sql.encode(self.encoding)
   else:
    sql=sql.encode(self.encoding,'surrogateescape')
  self._execute_command(COMMAND.COM_QUERY,sql)
  self._affected_rows=self._read_query_result(unbuffered=unbuffered)
  return self._affected_rows
 def next_result(self,unbuffered=False):
  self._affected_rows=self._read_query_result(unbuffered=unbuffered)
  return self._affected_rows
 def affected_rows(self):
  return self._affected_rows
 def kill(self,thread_id):
  arg=struct.pack('<I',thread_id)
  self._execute_command(COMMAND.COM_PROCESS_KILL,arg)
  return self._read_ok_packet()
 def ping(self,reconnect=True):
  if self._sock is None:
   if reconnect:
    self.connect()
    reconnect=False
   else:
    raise err.Error("Already closed")
  try:
   self._execute_command(COMMAND.COM_PING,"")
   self._read_ok_packet()
  except Exception:
   if reconnect:
    self.connect()
    self.ping(False)
   else:
    raise
 def set_charset(self,charset):
  encoding=charset_by_name(charset).encoding
  self._execute_command(COMMAND.COM_QUERY,"SET NAMES %s"%self.escape(charset))
  self._read_packet()
  self.charset=charset
  self.encoding=encoding
 def connect(self,sock=None):
  self._closed=False
  try:
   if sock is None:
    if self.unix_socket:
     sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
     sock.settimeout(self.connect_timeout)
     sock.connect(self.unix_socket)
     self.host_info="Localhost via UNIX socket"
     self._secure=True
     if DEBUG:print('connected using unix_socket')
    else:
     kwargs={}
     if self.bind_address is not None:
      kwargs['source_address']=(self.bind_address,0)
     while True:
      try:
       sock=socket.create_connection((self.host,self.port),self.connect_timeout,**kwargs)
       break
      except(OSError,IOError)as e:
       if e.errno==errno.EINTR:
        continue
       raise
     self.host_info="socket %s:%d"%(self.host,self.port)
     if DEBUG:print('connected using socket')
     sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
    sock.settimeout(None)
    sock.setsockopt(socket.SOL_SOCKET,socket.SO_KEEPALIVE,1)
   self._sock=sock
   self._rfile=_makefile(sock,'rb')
   self._next_seq_id=0
   self._get_server_information()
   self._request_authentication()
   if self.sql_mode is not None:
    c=self.cursor()
    c.execute("SET sql_mode=%s",(self.sql_mode,))
   if self.init_command is not None:
    c=self.cursor()
    c.execute(self.init_command)
    c.close()
    self.commit()
   if self.autocommit_mode is not None:
    self.autocommit(self.autocommit_mode)
  except BaseException as e:
   self._rfile=None
   if sock is not None:
    try:
     sock.close()
    except: 
     pass
   if isinstance(e,(OSError,IOError,socket.error)):
    exc=err.OperationalError(2003,"Can't connect to MySQL server on %r (%s)"%(self.host,e))
    exc.original_exception=e
    exc.traceback=traceback.format_exc()
    if DEBUG:print(exc.traceback)
    raise exc
   raise
 def write_packet(self,payload):
  data=pack_int24(len(payload))+int2byte(self._next_seq_id)+payload
  if DEBUG:dump_packet(data)
  self._write_bytes(data)
  self._next_seq_id=(self._next_seq_id+1)%256
 def _read_packet(self,packet_type=MysqlPacket):
  buff=b''
  while True:
   packet_header=self._read_bytes(4)
   btrl,btrh,packet_number=struct.unpack('<HBB',packet_header)
   bytes_to_read=btrl+(btrh<<16)
   if packet_number!=self._next_seq_id:
    self._force_close()
    if packet_number==0:
     raise err.OperationalError(CR.CR_SERVER_LOST,"Lost connection to MySQL server during query")
    raise err.InternalError("Packet sequence number wrong - got %d expected %d"%(packet_number,self._next_seq_id))
   self._next_seq_id=(self._next_seq_id+1)%256
   recv_data=self._read_bytes(bytes_to_read)
   if DEBUG:dump_packet(recv_data)
   buff+=recv_data
   if bytes_to_read==0xffffff:
    continue
   if bytes_to_read<MAX_PACKET_LEN:
    break
  packet=packet_type(buff,self.encoding)
  packet.check_error()
  return packet
 def _read_bytes(self,num_bytes):
  self._sock.settimeout(self._read_timeout)
  while True:
   try:
    data=self._rfile.read(num_bytes)
    break
   except(IOError,OSError)as e:
    if e.errno==errno.EINTR:
     continue
    self._force_close()
    raise err.OperationalError(CR.CR_SERVER_LOST,"Lost connection to MySQL server during query (%s)"%(e,))
   except BaseException:
    self._force_close()
    raise
  if len(data)<num_bytes:
   self._force_close()
   raise err.OperationalError(CR.CR_SERVER_LOST,"Lost connection to MySQL server during query")
  return data
 def _write_bytes(self,data):
  self._sock.settimeout(self._write_timeout)
  try:
   self._sock.sendall(data)
  except IOError as e:
   self._force_close()
   raise err.OperationalError(CR.CR_SERVER_GONE_ERROR,"MySQL server has gone away (%r)"%(e,))
 def _read_query_result(self,unbuffered=False):
  self._result=None
  if unbuffered:
   try:
    result=MySQLResult(self)
    result.init_unbuffered_query()
   except:
    result.unbuffered_active=False
    result.connection=None
    raise
  else:
   result=MySQLResult(self)
   result.read()
  self._result=result
  if result.server_status is not None:
   self.server_status=result.server_status
  return result.affected_rows
 def insert_id(self):
  if self._result:
   return self._result.insert_id
  else:
   return 0
 def _execute_command(self,command,sql):
  if not self._sock:
   raise err.InterfaceError("(0, '')")
  if self._result is not None:
   if self._result.unbuffered_active:
    warnings.warn("Previous unbuffered result was left incomplete")
    self._result._finish_unbuffered_query()
   while self._result.has_next:
    self.next_result()
   self._result=None
  if isinstance(sql,text_type):
   sql=sql.encode(self.encoding)
  packet_size=min(MAX_PACKET_LEN,len(sql)+1) 
  prelude=struct.pack('<iB',packet_size,command)
  packet=prelude+sql[:packet_size-1]
  self._write_bytes(packet)
  if DEBUG:dump_packet(packet)
  self._next_seq_id=1
  if packet_size<MAX_PACKET_LEN:
   return
  sql=sql[packet_size-1:]
  while True:
   packet_size=min(MAX_PACKET_LEN,len(sql))
   self.write_packet(sql[:packet_size])
   sql=sql[packet_size:]
   if not sql and packet_size<MAX_PACKET_LEN:
    break
 def _request_authentication(self):
  if int(self.server_version.split('.',1)[0])>=5:
   self.client_flag|=CLIENT.MULTI_RESULTS
  if self.user is None:
   raise ValueError("Did not specify a username")
  charset_id=charset_by_name(self.charset).id
  if isinstance(self.user,text_type):
   self.user=self.user.encode(self.encoding)
  data_init=struct.pack('<iIB23s',self.client_flag,MAX_PACKET_LEN,charset_id,b'')
  if self.ssl and self.server_capabilities&CLIENT.SSL:
   self.write_packet(data_init)
   self._sock=self.ctx.wrap_socket(self._sock,server_hostname=self.host)
   self._rfile=_makefile(self._sock,'rb')
   self._secure=True
  data=data_init+self.user+b'\0'
  authresp=b''
  plugin_name=None
  if self._auth_plugin_name=='':
   plugin_name=b''
   authresp=_auth.scramble_native_password(self.password,self.salt)
  elif self._auth_plugin_name=='mysql_native_password':
   plugin_name=b'mysql_native_password'
   authresp=_auth.scramble_native_password(self.password,self.salt)
  elif self._auth_plugin_name=='caching_sha2_password':
   plugin_name=b'caching_sha2_password'
   if self.password:
    if DEBUG:
     print("caching_sha2: trying fast path")
    authresp=_auth.scramble_caching_sha2(self.password,self.salt)
   else:
    if DEBUG:
     print("caching_sha2: empty password")
  elif self._auth_plugin_name=='sha256_password':
   plugin_name=b'sha256_password'
   if self.ssl and self.server_capabilities&CLIENT.SSL:
    authresp=self.password+b'\0'
   elif self.password:
    authresp=b'\1' 
   else:
    authresp=b'\0' 
  if self.server_capabilities&CLIENT.PLUGIN_AUTH_LENENC_CLIENT_DATA:
   data+=lenenc_int(len(authresp))+authresp
  elif self.server_capabilities&CLIENT.SECURE_CONNECTION:
   data+=struct.pack('B',len(authresp))+authresp
  else: 
   data+=authresp+b'\0'
  if self.db and self.server_capabilities&CLIENT.CONNECT_WITH_DB:
   if isinstance(self.db,text_type):
    self.db=self.db.encode(self.encoding)
   data+=self.db+b'\0'
  if self.server_capabilities&CLIENT.PLUGIN_AUTH:
   data+=(plugin_name or b'')+b'\0'
  if self.server_capabilities&CLIENT.CONNECT_ATTRS:
   connect_attrs=b''
   for k,v in self._connect_attrs.items():
    k=k.encode('utf-8')
    connect_attrs+=struct.pack('B',len(k))+k
    v=v.encode('utf-8')
    connect_attrs+=struct.pack('B',len(v))+v
   data+=struct.pack('B',len(connect_attrs))+connect_attrs
  self.write_packet(data)
  auth_packet=self._read_packet()
  if auth_packet.is_auth_switch_request():
   if DEBUG:print("received auth switch")
   auth_packet.read_uint8()
   plugin_name=auth_packet.read_string()
   if self.server_capabilities&CLIENT.PLUGIN_AUTH and plugin_name is not None:
    auth_packet=self._process_auth(plugin_name,auth_packet)
   else:
    data=_auth.scramble_old_password(self.password,self.salt)+b'\0'
    self.write_packet(data)
    auth_packet=self._read_packet()
  elif auth_packet.is_extra_auth_data():
   if DEBUG:
    print("received extra data")
   if self._auth_plugin_name=="caching_sha2_password":
    auth_packet=_auth.caching_sha2_password_auth(self,auth_packet)
   elif self._auth_plugin_name=="sha256_password":
    auth_packet=_auth.sha256_password_auth(self,auth_packet)
   else:
    raise err.OperationalError("Received extra packet for auth method %r",self._auth_plugin_name)
  if DEBUG:print("Succeed to auth")
 def _process_auth(self,plugin_name,auth_packet):
  handler=self._get_auth_plugin_handler(plugin_name)
  if handler:
   try:
    return handler.authenticate(auth_packet)
   except AttributeError:
    if plugin_name!=b'dialog':
     raise err.OperationalError(2059,"Authentication plugin '%s'" " not loaded: - %r missing authenticate method"%(plugin_name,type(handler)))
  if plugin_name==b"caching_sha2_password":
   return _auth.caching_sha2_password_auth(self,auth_packet)
  elif plugin_name==b"sha256_password":
   return _auth.sha256_password_auth(self,auth_packet)
  elif plugin_name==b"mysql_native_password":
   data=_auth.scramble_native_password(self.password,auth_packet.read_all())
  elif plugin_name==b"mysql_old_password":
   data=_auth.scramble_old_password(self.password,auth_packet.read_all())+b'\0'
  elif plugin_name==b"mysql_clear_password":
   data=self.password+b'\0'
  elif plugin_name==b"dialog":
   pkt=auth_packet
   while True:
    flag=pkt.read_uint8()
    echo=(flag&0x06)==0x02
    last=(flag&0x01)==0x01
    prompt=pkt.read_all()
    if prompt==b"Password: ":
     self.write_packet(self.password+b'\0')
    elif handler:
     resp='no response - TypeError within plugin.prompt method'
     try:
      resp=handler.prompt(echo,prompt)
      self.write_packet(resp+b'\0')
     except AttributeError:
      raise err.OperationalError(2059,"Authentication plugin '%s'" " not loaded: - %r missing prompt method"%(plugin_name,handler))
     except TypeError:
      raise err.OperationalError(2061,"Authentication plugin '%s'" " %r didn't respond with string. Returned '%r' to prompt %r"%(plugin_name,handler,resp,prompt))
    else:
     raise err.OperationalError(2059,"Authentication plugin '%s' (%r) not configured"%(plugin_name,handler))
    pkt=self._read_packet()
    pkt.check_error()
    if pkt.is_ok_packet()or last:
     break
   return pkt
  else:
   raise err.OperationalError(2059,"Authentication plugin '%s' not configured"%plugin_name)
  self.write_packet(data)
  pkt=self._read_packet()
  pkt.check_error()
  return pkt
 def _get_auth_plugin_handler(self,plugin_name):
  plugin_class=self._auth_plugin_map.get(plugin_name)
  if not plugin_class and isinstance(plugin_name,bytes):
   plugin_class=self._auth_plugin_map.get(plugin_name.decode('ascii'))
  if plugin_class:
   try:
    handler=plugin_class(self)
   except TypeError:
    raise err.OperationalError(2059,"Authentication plugin '%s'" " not loaded: - %r cannot be constructed with connection object"%(plugin_name,plugin_class))
  else:
   handler=None
  return handler
 def thread_id(self):
  return self.server_thread_id[0]
 def character_set_name(self):
  return self.charset
 def get_host_info(self):
  return self.host_info
 def get_proto_info(self):
  return self.protocol_version
 def _get_server_information(self):
  i=0
  packet=self._read_packet()
  data=packet.get_all_data()
  self.protocol_version=byte2int(data[i:i+1])
  i+=1
  server_end=data.find(b'\0',i)
  self.server_version=data[i:server_end].decode('latin1')
  i=server_end+1
  self.server_thread_id=struct.unpack('<I',data[i:i+4])
  i+=4
  self.salt=data[i:i+8]
  i+=9 
  self.server_capabilities=struct.unpack('<H',data[i:i+2])[0]
  i+=2
  if len(data)>=i+6:
   lang,stat,cap_h,salt_len=struct.unpack('<BHHB',data[i:i+6])
   i+=6
   self.server_language=lang
   try:
    self.server_charset=charset_by_id(lang).name
   except KeyError:
    self.server_charset=None
   self.server_status=stat
   if DEBUG:print("server_status: %x"%stat)
   self.server_capabilities|=cap_h<<16
   if DEBUG:print("salt_len:",salt_len)
   salt_len=max(12,salt_len-9)
  i+=10
  if len(data)>=i+salt_len:
   self.salt+=data[i:i+salt_len]
   i+=salt_len
  i+=1
  if self.server_capabilities&CLIENT.PLUGIN_AUTH and len(data)>=i:
   server_end=data.find(b'\0',i)
   if server_end<0:
    self._auth_plugin_name=data[i:].decode('utf-8')
   else:
    self._auth_plugin_name=data[i:server_end].decode('utf-8')
 def get_server_info(self):
  return self.server_version
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
class MySQLResult(object):
 def __init__(self,connection):
  self.connection=connection
  self.affected_rows=None
  self.insert_id=None
  self.server_status=None
  self.warning_count=0
  self.message=None
  self.field_count=0
  self.description=None
  self.rows=None
  self.has_next=None
  self.unbuffered_active=False
 def __del__(self):
  if self.unbuffered_active:
   self._finish_unbuffered_query()
 def read(self):
  try:
   first_packet=self.connection._read_packet()
   if first_packet.is_ok_packet():
    self._read_ok_packet(first_packet)
   elif first_packet.is_load_local_packet():
    self._read_load_local_packet(first_packet)
   else:
    self._read_result_packet(first_packet)
  finally:
   self.connection=None
 def init_unbuffered_query(self):
  self.unbuffered_active=True
  first_packet=self.connection._read_packet()
  if first_packet.is_ok_packet():
   self._read_ok_packet(first_packet)
   self.unbuffered_active=False
   self.connection=None
  elif first_packet.is_load_local_packet():
   self._read_load_local_packet(first_packet)
   self.unbuffered_active=False
   self.connection=None
  else:
   self.field_count=first_packet.read_length_encoded_integer()
   self._get_descriptions()
   self.affected_rows=18446744073709551615
 def _read_ok_packet(self,first_packet):
  ok_packet=OKPacketWrapper(first_packet)
  self.affected_rows=ok_packet.affected_rows
  self.insert_id=ok_packet.insert_id
  self.server_status=ok_packet.server_status
  self.warning_count=ok_packet.warning_count
  self.message=ok_packet.message
  self.has_next=ok_packet.has_next
 def _read_load_local_packet(self,first_packet):
  if not self.connection._local_infile:
   raise RuntimeError("**WARN**: Received LOAD_LOCAL packet but local_infile option is false.")
  load_packet=LoadLocalPacketWrapper(first_packet)
  sender=LoadLocalFile(load_packet.filename,self.connection)
  try:
   sender.send_data()
  except:
   self.connection._read_packet() 
   raise
  ok_packet=self.connection._read_packet()
  if not ok_packet.is_ok_packet():
   raise err.OperationalError(2014,"Commands Out of Sync")
  self._read_ok_packet(ok_packet)
 def _check_packet_is_eof(self,packet):
  if not packet.is_eof_packet():
   return False
  wp=EOFPacketWrapper(packet)
  self.warning_count=wp.warning_count
  self.has_next=wp.has_next
  return True
 def _read_result_packet(self,first_packet):
  self.field_count=first_packet.read_length_encoded_integer()
  self._get_descriptions()
  self._read_rowdata_packet()
 def _read_rowdata_packet_unbuffered(self):
  if not self.unbuffered_active:
   return
  packet=self.connection._read_packet()
  if self._check_packet_is_eof(packet):
   self.unbuffered_active=False
   self.connection=None
   self.rows=None
   return
  row=self._read_row_from_packet(packet)
  self.affected_rows=1
  self.rows=(row,) 
  return row
 def _finish_unbuffered_query(self):
  while self.unbuffered_active:
   packet=self.connection._read_packet()
   if self._check_packet_is_eof(packet):
    self.unbuffered_active=False
    self.connection=None 
 def _read_rowdata_packet(self):
  rows=[]
  while True:
   packet=self.connection._read_packet()
   if self._check_packet_is_eof(packet):
    self.connection=None 
    break
   rows.append(self._read_row_from_packet(packet))
  self.affected_rows=len(rows)
  self.rows=tuple(rows)
 def _read_row_from_packet(self,packet):
  row=[]
  for encoding,converter in self.converters:
   try:
    data=packet.read_length_coded_string()
   except IndexError:
    break
   if data is not None:
    if encoding is not None:
     data=data.decode(encoding)
    if DEBUG:print("DEBUG: DATA = ",data)
    if converter is not None:
     data=converter(data)
   row.append(data)
  return tuple(row)
 def _get_descriptions(self):
  self.fields=[]
  self.converters=[]
  use_unicode=self.connection.use_unicode
  conn_encoding=self.connection.encoding
  description=[]
  for i in range_type(self.field_count):
   field=self.connection._read_packet(FieldDescriptorPacket)
   self.fields.append(field)
   description.append(field.description())
   field_type=field.type_code
   if use_unicode:
    if field_type==FIELD_TYPE.JSON:
     encoding=conn_encoding 
    elif field_type in TEXT_TYPES:
     if field.charsetnr==63: 
      encoding=None
     else:
      encoding=conn_encoding
    else:
     encoding='ascii'
   else:
    encoding=None
   converter=self.connection.decoders.get(field_type)
   if converter is converters.through:
    converter=None
   if DEBUG:print("DEBUG: field={}, converter={}".format(field,converter))
   self.converters.append((encoding,converter))
  eof_packet=self.connection._read_packet()
  assert eof_packet.is_eof_packet(),'Protocol error, expecting EOF'
  self.description=tuple(description)
class LoadLocalFile(object):
 def __init__(self,filename,connection):
  self.filename=filename
  self.connection=connection
 def send_data(self):
  if not self.connection._sock:
   raise err.InterfaceError("(0, '')")
  conn=self.connection
  try:
   with open(self.filename,'rb')as open_file:
    packet_size=min(conn.max_allowed_packet,16*1024) 
    while True:
     chunk=open_file.read(packet_size)
     if not chunk:
      break
     conn.write_packet(chunk)
  except IOError:
   raise err.OperationalError(1017,"Can't find file '{0}'".format(self.filename))
  finally:
   conn.write_packet(b'')
