"""Memcached client for use with storememcached.MemcachedStorageManager."""

try:
    import cPickle as pickle
except ImportError:
    import pickle
import socket
import threading
import time
try:
    # Apparently, zlib can be disabled when compiling Python.
    import zlib
except ImportError:
    zlib = None



class Flagset(object):
    """A set of coercion rules for memcache data encoding.
    
    compress_threshold: an int which declares the minimum length (in bytes)
        of cache values to compress. If 0 (the default), no values will
        be compressed. Setting this to a nonzero value can save time for
        both getting and setting; tune it to the breakpoint where the
        computational cost of compression (and decompression) exceeds
        the savings due to reduced I/O and memory usage.
    
    compress_savings: a float which declares the minimum savings
        (as a fraction of 1.0) required to cache compressed values.
        Defaults to 0.2; that is, compressed values must be 20% smaller
        than the uncompressed value in order to store the compressed
        value instead of the uncompressed value. Since compression is
        always performed even if the uncompressed value is stored
        (assuming we're above the compress_threshold), this works best
        when the rate of gets exceeds the rate of sets by a large margin,
        so that the cost of compression is offset by the savings due to
        skipping decompression.
    """
    
    FLAG_PICKLE = 1
    FLAG_INT = 2
    FLAG_LONG = 4
    FLAG_COMPRESS = 8
    
    def __init__(self, compress_threshold=0, compress_savings=0.2):
        self.compress_threshold = compress_threshold
        self.compress_savings = compress_savings
    
    def push(self, value):
        """Return (flags, coerced value) for output."""
        if isinstance(value, int):
            typeflag = self.FLAG_INT
            value = str(value)
        elif isinstance(value, long):
            typeflag = self.FLAG_LONG
            value = str(value)
        elif isinstance(value, basestring):
            typeflag = 0
        else:
            typeflag = self.FLAG_PICKLE
            value = pickle.dumps(value, 2)
        
        compressflag = 0
        if zlib and self.compress_threshold:
            original_len = len(value)
            if original_len > self.compress_threshold:
                try:
                    c_val = zlib.compress(value)
                    if len(c_val) < original_len * (1 - self.compress_savings):
                        compressflag = self.FLAG_COMPRESS
                        value = c_val
                except zlib.error:
                    pass
        
        return (typeflag | compressflag), value
    
    def pull(self, value, flags):
        """Return value, coerced for input according to the given flags."""
        if flags == 0:
            return value
        
        if flags & self.FLAG_COMPRESS:
            if not zlib:
                # woops! no zlib available
                return None
            try:
                value = zlib.decompress(value)
            except zlib.error:
                # invalid compressed data
                return None
        
        if flags & self.FLAG_INT:
            return int(value)
        if flags & self.FLAG_LONG:
            return long(value)
        if flags & self.FLAG_PICKLE:
            try:
                return pickle.loads(value)
            except:
                return None
        raise ValueError("unknown flags %x for value %r" % (flags, value))


class Client(object):
    """Memcache client interface.
    
    servers: a sequence of "host[:port]" strings.
    """
    
    flagset = Flagset()
    
    def __init__(self, servers):
        self.servers = [Server(s) for s in servers]
    
    def _key_to_server(self, key):
        """returns a server without locking it"""
        return self.servers[hash(key) % len(self.servers)]
    
    def delete(self, key, time=0):
        """Delete the cached value for the given key."""
        if time:
            cmd = "delete %s %d" % (key, time)
        else:
            cmd = "delete %s" % key
        server = self._key_to_server(key)
        server.acquire()
        try:
            if server.assert_socket():
                r = server.store(cmd)
                if r not in ('DELETED', 'NOT_FOUND'):
                    raise IOError(r)
        finally:
            server.release()
    
    def _set(self, cmd, key, val, time):
        server = self._key_to_server(key)
        server.acquire()
        try:
            if server.assert_socket():
                flags, val = self.flagset.push(val)
                msg = ("%s %s %d %d %d\r\n%s" %
                       (cmd, key, flags, time, len(val), val))
                r = server.store(msg)
                if r != "STORED":
                    raise IOError(r)
        finally:
            server.release()
    
    def add(self, key, val, time=0):
        """Add a new key/value pair to the cache (unless it exists)."""
        self._set("add", key, val, time)
    
    def replace(self, key, val, time=0):
        """Replace an existing key/value pair, only if it exists."""
        self._set("replace", key, val, time)
    
    def set(self, key, val, time=0):
        """Set the given key/value pair."""
        self._set("set", key, val, time)
    
    def get(self, key):
        """Retrieve the cached value for the given key."""
        server = self._key_to_server(key)
        server.acquire()
        try:
            if server.assert_socket():
                server.retrieve("get %s" % key)
                line = server.readline()
                if not line.startswith('VALUE'):
                    return None
                
                _, _, flags, length = line.split()
                buf = server.recv(int(length) + 2)[:-2]
                
                if server.readline() != "END":
                    return None
                return self.flagset.pull(buf, int(flags))
        finally:
            server.release()
    
    def get_multi(self, keys):
        """Return a dict of cached values for the given keys.
        
        The returned dict will contain only those entries which
        were found in the cache.
        """
        # the list of memcached servers we need
        servers = {}
        for key in keys:
            server = self._key_to_server(key)
            if server not in servers:
                servers[server] = [str(key)]
            else:
                servers[server].append(str(key))
        
        results = {}
        for server, subkeys in servers.items():
            server.acquire()
            try:
                if not server.assert_socket():
                    continue
                
                try:
                    server.retrieve("get %s" % " ".join(subkeys))
                    line = server.readline()
                    while line and line.strip() != 'END':
                        _, key, flags, length = line.split()
                        buf = server.recv(int(length) + 2)[:-2]
                        results[key] = self.flagset.pull(buf, int(flags))
                        line = server.readline()
                except socket.error:
                    # dead server
                    pass
            finally:
                try:
                    server.release()
                except:
                    pass
        
        return results
    
    def flush_all(self):
        """Expire all data currently in the memcache servers."""
        for s in self.servers:
            s.acquire()
            try:
                if s.assert_socket():
                    s.flush_all()
            finally:
                s.release()
    
    def disconnect_all(self):
        """Disconnect from all servers."""
        for s in self.servers:
            s.close()


class Server(object):
    
    deaduntil = 0
    socket = None
    retry_delay = 5
    
    # Replace this with a log(msg) function if desired.
    # This is *so* much faster than using the logging module;
    # please don't ever replace it with that. You can hook the
    # logging module in here if you want (per deployment),
    # but don't hardwire it.
    log = None
    
    def __init__(self, host):
        if ":" in host:
            self.ip, port = host.split(":")
            self.port = int(port)
        else:
            self.ip, self.port = host, 11211
        self._socket_lock = threading.Lock()
    
    def mark_dead(self):
        self.deaduntil = time.time() + self.retry_delay
        self.close()
    
    def acquire(self):
        if self.log:
            self.log("Acquiring %s:%s." % (self.ip, self.port))
        # B L O C K
        self._socket_lock.acquire(True)
        if self.log:
            self.log("I now own %s:%s." % (self.ip, self.port))
    
    def assert_socket(self):
        if self.deaduntil and self.deaduntil > time.time():
            return False
        else:
            self.deaduntil = 0
        
        if not self.socket:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            try:
                if self.log:
                    self.log("Connecting to %s:%s." % (self.ip, self.port))
                s.connect((self.ip, self.port))
            except (socket.error, socket.timeout), exc:
                if self.log:
                    self.log("Error connecting to %s:%s.\n%r" %
                             (self.ip, self.port, exc))
                self.mark_dead()
                return False
            self.socket = s
        
        return True
    
    def release(self):
        """Release a connection."""
        if self.log:
            self.log("Releasing %s:%s." % (self.ip, self.port))
        self._socket_lock.release()
    
    def close(self):
        if self.socket:
            if self.log:
                self.log("Closing %s:%s." % (self.ip, self.port))
            self.socket.close()
            self.socket = None
    
    def store(self, command):
        """Send the given command string and return its single-line response."""
        try:
            if self.log:
                self.log("> %s" % command)
            self.socket.sendall(command + '\r\n')
        except socket.error, exc:
            self.mark_dead()
            raise
        return self.readline()
    
    def retrieve(self, command):
        """Send the given command string and let the caller recv the response."""
        try:
            if self.log:
                self.log("> %s" % command)
            self.socket.sendall(command + '\r\n')
        except socket.error, exc:
            self.mark_dead()
            raise
    
    def readline(self):
        try:
            buffers = ''
            recv = self.socket.recv
            while 1:
                data = recv(1)
                if not data:
                    self.mark_dead()
                    break
                if data == '\n' and buffers and buffers[-1] == '\r':
                    if self.log:
                        self.log("< %s" % buffers[:-1])
                    return(buffers[:-1])
                buffers = buffers + data
            if self.log:
                self.log("< %s" % buffers)
            return buffers
        except socket.error, exc:
            self.mark_dead()
            raise
    
    def recv(self, size):
        try:
            buf = ''
            buflen = 0
            recv = self.socket.recv
            while True:
                buf += recv(size - buflen)
                buflen = len(buf)
                if buflen >= size:
                    break
            if self.log:
                self.log("< %s" % buf)
            if buflen != size:
                raise IOError("%d bytes received (expected %d)." %
                              (buflen, size), buf)
            if not buf.endswith('\r\n'):
                raise IOError("Response did not end with CRLF.", buf)
            
            return buf
        except socket.error, exc:
            self.mark_dead()
            raise
    
    def flush_all(self):
        """Expire all data currently in the server."""
        r = self.store('flush_all')
        if r != "OK":
            raise IOError(r)
    
    def __repr__(self):
        d = ''
        if self.deaduntil:
            d = " (dead until %d)" % self.deaduntil
        return "<memcached at %s port: %d%s>" % (self.ip, self.port, d)


