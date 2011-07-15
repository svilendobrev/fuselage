from operations import Op2, Operations
from log import log
import os
import re

class id_and_name:
    delim = '___'
    re_id = re.compile('^[0-9]+'+delim)

    @classmethod
    def make( klas, id_, name):
        return id_and_name.delim.join( [ str(id_ or 0), name ])

    if 0:
        @staticmethod
        def split( item_name):
            id_, name = item_name.split( id_and_name.delim, 1)
            return id_, name

    @classmethod
    def get_name( klas, s):
        return klas.re_id.sub( '', s, 1)

    @classmethod
    def get_id( klas, s):
        r = klas.re_id.match(s)
        if not r: return None
        return int(s[ :r.end()-len(klas.delim)])

    @classmethod
    def strip_id_from_path( klas, path):
        p, filename = os.path.split( path)
        stripped = klas.get_name( filename)
        return os.path.join( p, stripped)

    @classmethod
    def get_id_from_path( klas, path):
        p, filename = os.path.split( path)
        return klas.get_id( filename)


class Aspect( object):
    _rexp = None
    _dir_name = None

    class External( object):
        def __init__(me, path):
            me.path = path

    @classmethod
    def rexp( klas):
        if vars(klas).get('_rexp') is None:
            klas._rexp = re.compile('^/'+klas.dir_name())
        return klas._rexp

    @classmethod
    def dir_name( klas):
        if vars(klas).get('_dir_name') is None:
            klas._dir_name = klas.__name__.lower()
        return klas._dir_name

    aliases = dict() # cache for getattr to avoid redundant parsing and queries
    alias_path_in_use = None

    class Parser( object):
        error = None
        def __init__( me, context, op_klas, full_path, path_in_aspect):
            me.context = context
            me.db = context.db
            me.op_klas = op_klas
            me.full_path        = full_path      # the complete path that generated the query e.g. <MPOINT>/t1/+/t2/=/object/path.txt
            me.path_in_aspect   = path_in_aspect # after stripping aspect regex from full path
            me.init()

        def init( me):
            pass

        def check4alias( me):
            if not issubclass( me.op_klas, Operations.getattr):
                Aspect.aliases.clear()
                me.alias_path_in_use = None
                return None
            if me.full_path != Aspect.alias_path_in_use:
                Aspect.aliases.pop( Aspect.alias_path_in_use, None)
                Aspect.alias_path_in_use = me.full_path
            return Aspect.aliases.get( me.full_path, None)

        def setup_from_alias( me, obj):
            pass

        def parse( me):
            if not me.is_external():
                if me.path_in_aspect is None:
                    return 'malformed'
                if not me.path_in_aspect:
                    return 'aspect'
                log('len aliases:', len( Aspect.aliases))
                obj = me.check4alias()
                if obj:
                    log('found alias:', obj)
                    return me.setup_from_alias( obj)
                return me._parse()
            return Aspect.External( me.full_path)

        def _parse( me):
            pass

        def is_external( me):
            return (me.path_in_aspect is None
                and issubclass( me.op_klas, Op2)
                and not me.full_path.startswith( me.context.mountpoint))

    class Error:
        def __init__( me, data):
            me.data = data
        def __str__(me):
            return me.__class__.__name__ + ' ' + str(me.data)

    @classmethod
    def init( me, context):
        pass

##################

class Stats( Aspect):
    rexp = '^/stats'


# vim:ts=4:sw=4:expandtab
