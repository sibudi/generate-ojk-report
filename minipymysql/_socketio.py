from socket import*
import io
import errno
__all__=['SocketIO']
EINTR=errno.EINTR
_blocking_errnos=(errno.EAGAIN,errno.EWOULDBLOCK)
class SocketIO(io.RawIOBase):
 def __init__(self,sock,mode):
  if mode not in("r","w","rw","rb","wb","rwb"):
   raise ValueError("invalid mode: %r"%mode)
  io.RawIOBase.__init__(self)
  self._sock=sock
  if "b" not in mode:
   mode+="b"
  self._mode=mode
  self._reading="r" in mode
  self._writing="w" in mode
  self._timeout_occurred=False
 def readinto(self,b):
  self._checkClosed()
  self._checkReadable()
  if self._timeout_occurred:
   raise IOError("cannot read from timed out object")
  while True:
   try:
    return self._sock.recv_into(b)
   except timeout:
    self._timeout_occurred=True
    raise
   except error as e:
    n=e.args[0]
    if n==EINTR:
     continue
    if n in _blocking_errnos:
     return None
    raise
 def write(self,b):
  self._checkClosed()
  self._checkWritable()
  try:
   return self._sock.send(b)
  except error as e:
   if e.args[0]in _blocking_errnos:
    return None
   raise
 def readable(self):
  if self.closed:
   raise ValueError("I/O operation on closed socket.")
  return self._reading
 def writable(self):
  if self.closed:
   raise ValueError("I/O operation on closed socket.")
  return self._writing
 def seekable(self):
  if self.closed:
   raise ValueError("I/O operation on closed socket.")
  return super().seekable()
 def fileno(self):
  self._checkClosed()
  return self._sock.fileno()
 @property
 def name(self):
  if not self.closed:
   return self.fileno()
  else:
   return-1
 @property
 def mode(self):
  return self._mode
 def close(self):
  if self.closed:
   return
  io.RawIOBase.close(self)
  self._sock._decref_socketios()
  self._sock=None
