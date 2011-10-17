from log import log
from os.path import isfile, basename, dirname, exists, join
from stat import *
from posix import stat_result
import os
import fuse
from errno import *
import errno
import itertools as it

class BaseOp( object):
    malformed = ENOENT
    not_implemented = ENOSYS
    osfunc = None
    def __init__( me, context):
        me.context = context
        me.db = context.db

    def osexec( me, *args):
        func = me.osfunc or getattr(os, me.__class__.__name__, None)
        log('os', func, *args)
        try:
            return func( *args)
        except OSError, e:
            return e.errno

class Op1( BaseOp):
    init_attrs = ''
    def __init__( me, context, q, *args):
        BaseOp.__init__( me, context)
        me.q = q
        names = me.init_attrs.split()
        me.args = args
        log('init op:', me.__class__.__name__, names, repr(args))
        #assert len(names) == len(args), str(names) + ' '+repr(args)
        for name, value in zip( names, args):
            setattr( me, name, value)

class Op2( BaseOp):
    def __init__( me, context, q1, q2):
        BaseOp.__init__( me, context)
        me.q1 = q1
        me.q2 = q2
        log('init op:', me.__class__.__name__, q1.full_path, q2.full_path)

class Operations:
    class getattr( Op1):
        osfunc = os.lstat
        def aspect( me):
            return me.osexec( me.context.archive)
        def as_file( me, stat_res):
            return me._as_type( S_IFREG, stat_res)
        def as_dir( me, stat_res):
            return me._as_type( S_IFDIR, stat_res)
        def as_symlink( me, stat_res):
            return me._as_type( S_IFLNK, stat_res)
        def _as_type( me, typ, stat_res):
            if isinstance( stat_res, stat_result):
                all = list(stat_res)
                all[0] = typ | S_IMODE( all[0]) # show rtag as file
                stat_res = stat_result( all)
            return stat_res

    class readlink( Op1):
        not_implemented = EINVAL
    class unlink( Op1):
        not_implemented = EROFS
    class rmdir( Op1):
        pass
    class rename( Op2):
        pass
    class symlink( Op2):
        not_implemented = EINVAL
    class link( Op2):
        not_implemented = EINVAL
    class readdir( Op1):
        osfunc = os.listdir
        init_attrs = 'offset'
        entries = ['.', '..']
        def direntries( me, entries):
            for e in it.chain( me.entries, entries):
                if isinstance(e,unicode):
                    e = e.encode('utf-8')
                yield fuse.Direntry(e)

    class access( Op1):
        init_attrs = 'mode'
        def aspect( me):
            return None if os.access( me.context.archive, getattr(me, 'mode', 0)) else EACCES

    class mknod( Op1):
        malformed = EFAULT
        not_implemented = EROFS
        init_attrs = 'mode dev'
    class mkdir( Op1):
        init_attrs = 'mode'
    class chmod( Op1):
        init_attrs = 'mode'
        not_implemented = EROFS
    class create( Op1):
        init_attrs = 'fi_flags mode'
    class open( Op1):
        init_attrs = 'flags'
        not_implemented = EROFS
    class utime( Op1):
        init_attrs = 'times'
    class chown( Op1):
        init_attrs = 'user group'
        not_implemented = EROFS
    class truncate( Op1):
        init_attrs = 'length'
        not_implemented = EROFS


# vim:ts=4:sw=4:expandtab
