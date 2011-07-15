from root import Root
from log import log
from operations import Operations, Op1, Op2
from util.attr import issubclass
from posix import stat_result
from stat import S_IFLNK
import os

class Hardlink( Root):
    getattr4symlink = False

    def _dispatch( me):
        path = os.path.sep + me.q.path_in_aspect
        return me.context.get_op( me.q.op_klas)( path, *me.args)

    def _external_dispatch( me, ext):
        path2 = os.path.sep + me.q2.path_in_aspect
        op_klas = me.q2.op_klas
        if issubclass( op_klas, Operations.symlink):
            Hardlink.getattr4symlink = True
            op_klas = Operations.link
        return me.context.get_op( op_klas)( ext.path, path2)

    class Parser( Root.Parser):
        def _parse( me):
            return 'dispatch'

    class readdir( Root.readdir):
        def _get_aspect_dirs(me):
            return [ name for name in Root.readdir._get_aspect_dirs(me) if name != Hardlink.dir_name() ]


class getattr_( Root.getattr):
    _dispatch = Hardlink._dispatch.im_func
    def dispatch( me):
        stat_res = me._dispatch()
        #if os.path.split( me.q.path_in_aspect)[0] and isinstance( stat_res, stat_result):
        if Hardlink.getattr4symlink and isinstance( stat_res, stat_result):
            all = list(stat_res)
            log('original:', all)
            all[0] = all[0] | S_IFLNK
            log('new:', all)
            stat_res = stat_result( all)
        Hardlink.getattr4symlink = False
        return stat_res

Hardlink.getattr = getattr_

for op_name, op_klas in vars(Operations).iteritems():
    if not issubclass( op_klas, (Op1, Op2)): continue
    klas = getattr( Hardlink, op_name, op_klas)
    A = None
    if issubclass( op_klas, Op1) and not getattr( klas, 'dispatch', None):
        class A( klas):
            dispatch = Hardlink._dispatch.im_func
    if issubclass( op_klas, Op2) and not getattr( klas, 'external_dispatch', None):
        class A( klas):
            external_dispatch = Hardlink._external_dispatch.im_func
    if A:
        setattr( Hardlink, op_name, A)


# vim:ts=4:sw=4:expandtab
