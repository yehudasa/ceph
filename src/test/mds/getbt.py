#!/usr/bin/env python

import sys
import rados
import struct

r = rados.Rados(conffile=sys.argv[1])
r.connect()
i = r.open_ioctx(sys.argv[2])
v = i.get_xattr(sys.argv[3], sys.argv[4])
i.close()
r.shutdown()

# stupid unpack_from requires a buffer of at least 20 bytes
b = v + '                       '
(ver, ino, size) = struct.unpack_from('<BQL', b)
print 'version: {v}'.format(v=ver)
print 'ino: {i}'.format(i=ino)
print 'size: {s}'.format(s=size)

if size > 0:
    print 'trace:'
off = 13
for i in range(0, size):
    (dino, dname_len) = struct.unpack_from('<QL', b, off)
    off += 12
    (dname, ver) = struct.unpack_from('<{l}sQ'.format(l=dname_len), b, off)
    off += dname_len + 8
    print '    - {ino: %d, name: %s, version: %d}' % (dino, dname, ver)
