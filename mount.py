#!/usr/bin/python
import fuse
from fuse import Fuse

import os
import sys
import traceback
from threading import RLock

import operations as op

import storage
from log import log
import log as _log
from util.attr import issubclass
from util.struct import DictAttr
import errno

from aspect import Aspect

fuse.fuse_python_api = (0, 2)

class TagFS( Fuse):
    archive = None
    repository      = None #config.default_repo
    logfile         = None #config.default_logfile
    storage_type    = None #config.default_storage_type
    query_grammar   = DictAttr()
    aspects = []

    @property
    def mountpoint( me):
        return me.fuse_args.mountpoint

    def __getattr__( me, name):
        klas = getattr( op.Operations, name, None)
        if not klas:
            return Fuse.__getattr__( me, name)
        #log( '__getattr__', name)
        return me.get_op( klas)

    def prepare_storage( me):
        mem = me.storage_type == 'memory'
        if mem:
            me.storage_type = 'sqlite'
        main_storage = getattr( storage, me.storage_type)
        for a in me.aspects: #TODO topologically sorted
            aspect_storage = getattr(a, me.storage_type, None)
            if not aspect_storage: continue
            main_storage.include( aspect_storage)
        main_storage.setup_listeners()
        me.db = main_storage( me, memory=mem)

    class Func1:
        def __init__( me, op_klas, context):
            me.ctx = context
            me.op_klas = op_klas

        def __call__( me, path, *args):
            if not isinstance( path, unicode): path = unicode( path, 'utf-8')
            ctx = me.ctx
            op_klas = me.op_klas
            log('------', op_klas.__name__, path, *args)
            d = me._from_path( path)
            if d.malformed:
                return -op_klas.malformed
            op_klas, func = me._get_op_func( d.asp, op_klas, d.typ)
            if not func:
                return -op_klas.not_implemented
            return me._exec( func, op_klas( ctx, d.q, *args), d.obj)

        def _exec( me, func, op, *operands):
            log('func:', func.__name__, op.__class__)
            res = None
            db = me.ctx.db
            try:
                res = func( op, *[ o for o in operands if o])
            except Exception, e:
                log('<<<<<<<<<<<<<<< Surprise:', e, '>>>>>>>>>>>', traceback.format_exc() )
                db.rollback()
            if not isinstance(res, bool) and res in errno.errorcode:
                res = -res
                db.rollback()
            else:
                db.commit()
            if isinstance(res,unicode):
                res = res.encode('utf-8')
            log('------ result:', repr(res))
            return res

        def _from_path( _me, path):
            asp, path_in_aspect = _me._get_aspect( path)
            q = asp.Parser( _me.ctx, _me.op_klas, path, path_in_aspect)
            obj = None
            typ = q.parse()
            if not isinstance( typ, str):
                obj = typ
                typ = typ.__class__.__name__.lower()
            malformed = typ == 'malformed'
            del _me
            return DictAttr( **locals())

        def _get_aspect( me, path):
            for a in me.ctx.aspects:
                z = a.rexp().match( path)
                if z:
                    path_in_asp = path[ z.end():]
                    if path_in_asp.startswith( os.path.sep):
                        path_in_asp = path_in_asp[ len(os.path.sep):]
                    log( 'aspect', a.__name__, 'path_in_aspect:', path_in_asp)
                    return a, path_in_asp
            return Aspect, None

        def _get_op_func( me, asp, op_klas, name):
            op_klas = getattr( asp, op_klas.__name__, op_klas)
            func = getattr( op_klas, name, None)
            return op_klas, func


    class Func2( Func1):
        def __call__( me, path1, path2):
            if not isinstance( path1, unicode): path1 = unicode( path1, 'utf-8')
            if not isinstance( path2, unicode): path2 = unicode( path2, 'utf-8')
            ctx = me.ctx
            op_klas = me.op_klas
            log( '======== ', op_klas.__name__, path1, path2)
            d1 = me._from_path( path1)
            d2 = me._from_path( path2)
            if d1.malformed or d2.malformed:
                return -op_klas.malformed
            fname = d1.typ +'_'+ d2.typ
            op_klas1, func1 = me._get_op_func( d1.asp, op_klas, fname)
            op_klas2, func2 = me._get_op_func( d2.asp, op_klas, fname)
            func = func1 or func2
            if not func:
                log('func:', fname + ' not implemented')
                return -op_klas.not_implemented
            op_klas = op_klas1 if func1 else op_klas2
            if func1 and func2:
                if issubclass( op_klas2, op_klas1): # mimic python rules for coercion
                    func = func2
                    op_klas = op_klas2
            return me._exec( func, op_klas( ctx, d1.q, d2.q), d1.obj, d2.obj)

    def get_op( me, op_klas):
        f = None
        if issubclass( op_klas, op.Op1):
            f = me.Func1( op_klas, me)
        elif issubclass( op_klas, op.Op2):
            f = me.Func2( op_klas, me)
        else:
            log( 'get_op unknown op_klas', op_klas)
            assert 0
        #log( 'get_op', op_klas.__name__, f.__class__.__name__)
        return f

    def statfs( me):
        log( 'statfs')
        return os.statvfs( me.archive)

    def fsinit( me, *args):
        log( 'fsinit', args)
        if 0:
            me.asgid = int( me.asgid)
            me.asuid = int( me.asuid)
            if me.asgid:
                os.setgid( me.asgid)
            if me.asuid:
                os.setuid( me.asuid)
        for a in me.aspects:
            a.init( me)
        return 0

    def fsdestroy ( me, *args):
        log( 'fsdestroy', args)
        return -errno.ENOSYS

    ##############

class Lock( object):
    def __init__( me):
        me.lock = RLock()
    def __enter__( me):
        me.lock.acquire()
        return me
    def __exit__( me, type, value, traceback):
        me.lock.release()

class FsFile( object):
    def __init__( me, path, flags, *mode):
        me.fd = os.open( path, flags)
        me.direct_io = 0
        me.keep_cache = 0
        me.lock = Lock()

    def read( me, length, offset):
        with me.lock:
            os.lseek( me.fd, offset, os.SEEK_SET)
            buf = os.read( me.fd, length)
            return buf

    def write( me, buf, offset):
        with me.lock:
            os.lseek( me.fd, offset, os.SEEK_SET)
            bytes = os.write( me.fd, buf)
            return bytes

    def release( me, flags):
        with me.lock:
            os.close( me.fd)

    def fsync( me, isfsyncfile):
        with me.lock:
            if isfsyncfile and hasattr( os, 'fdatasync'):
                os.fdatasync( me.fd)
            else:
                os.fsync( me.fd)

    def flush( me):
        with me.lock:
            os.close( os.dup( me.fd))

    def fgetattr( me):
        with me.lock:
            return os.fstat( me.fd)

    def ftruncate( me, len):
        with me.lock:
            os.ftruncate( me.fd, len)



from os.path import basename
def usage():
    return '''
    USAGE:
        {prog} [-d] [-o option1=value1] <mountpoint>
    PARAMETERS:
        -h - show full help with options
        -d - show verbose debug info
    '''.format( prog = basename( sys.argv[0]))

# USAGE ( fstab):
#     {prog}# <mount_point> fuse allow_other[,<options>] 0 0



def create_if_not_exists( name):
    name = os.path.normpath( os.path.expanduser( name))
    if not os.path.isdir( name):
        log('creating dir: ', name)
        try:
            os.makedirs( name, 755)
        except OSError, e:
            log('Cannot create: ', name, e)
            return None
    else:
        log('found dir: ', name)
    os.chmod( name, S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)
    #os.chmod( name, 755)
    return name


def create_repo( path, config):
    repo = create_if_not_exists( path or config.default_repo)
    if not repo:
        return
    arch = create_if_not_exists( os.path.join( repo, 'archive'))
    if not arch:
        return
    return repo, arch

from stat import *
def main( args, config):
    if len( args) == 1:
        print usage()
        return -1
    srv = TagFS( version='%prog ' + fuse.__version__, usage=usage(), dash_s_do='setsingle')
    srv.flags = 0
    srv.multithreaded = False
    srv.parser.add_option( mountopt='repository', metavar='PATH', default=config.default_repo, help='repository path [default: %s]' % config.default_repo)
    srv.parser.add_option( mountopt='logfile', metavar='FILE', default=config.default_logfile, help='log debug info to a file; leave empty to disable [default: %s]' % config.default_logfile)
    srv.parser.add_option( mountopt='storage_type', metavar='[sqlite|memory]', default=config.default_storage_type, help='storage backend: %s]' % config.default_storage_type)
    srv.parse( values=srv, errex=1)

    _log.DEBUG = '-d' in args
    _log.LOGFILE = srv.logfile
    d = create_repo( srv.repository, config)
    if not d: return
    srv.aspects = config.default_enabled_aspects
    srv.query_grammar.update( config.default_query_grammar)
    srv.repository, srv.archive = d
    if not srv.storage_type:
        srv.storage_type = config.default_storage_type
    srv.prepare_storage()
    srv.file_class = FsFile
    srv.main()
    return srv

if __name__ == '__main__':
    import config
    rval = main( sys.argv, config)
    if not rval:
        sys.exit( -1)

# vim:ts=4:sw=4:expandtab
