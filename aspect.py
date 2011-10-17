from operations import Op1, Op2, Operations
from log import log
from errno import *
import itertools as it
from util.attr import issubclass, isiterable
import os
import re

class id_and_name:
    delim = '___'
    re_id = re.compile('^[0-9]+'+delim)

    @classmethod
    def make( klas, id_, name):
        return id_and_name.delim.join( [ str(id_ or 0), name ])

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
            if not issubclass( me.op_klas, (Operations.getattr, Operations.readlink)):
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
        __repr__ = __str__


    @classmethod
    def init( me, context):
        pass

##################

class QAspectMeta( type):
    operandType = resultType = None
    def __init__( cls, name, bases, dct):
        if not cls.operandType or not cls.resultType:
            return
        operand = cls.operandType.__name__.lower()
        result = cls.resultType.__name__.lower()
        for op_name, op_klas in vars(QAspect).iteritems():
            op_klas = getattr( cls, op_name, op_klas)
            if issubclass( op_klas, (Op1,Op2)):
                class new_op_klas( op_klas): pass
                new_op_klas.__name__ = op_klas.__name__
                cls.rename_meth( op_klas, new_op_klas, 'operand', operand)
                cls.rename_meth( op_klas, new_op_klas, 'result', result)
                setattr( cls, op_name, new_op_klas)

        parser = cls.Parser
        parser.result = property( lambda me: getattr( me, result), lambda me, val: setattr( me, result, val))
        parser.operand = property( lambda me: getattr( me, operand), lambda me, val: setattr( me, operand, val))

    def rename_meth( cls, op_klas, new_op_klas, old, new):
        meth = getattr( op_klas, old, None)
        if meth and getattr( new_op_klas, new, None) is None:
            setattr( new_op_klas, new, meth)
        oldname = old+'_'+old
        newname = new+'_'+new
        meth = getattr( op_klas, oldname, None)
        if meth and vars( op_klas).get( newname) is None:
            setattr( new_op_klas, newname, meth)

class QAspect( Aspect):
    ''' abstraction of aspects that support queries with logic operations'''

    __metaclass__ = QAspectMeta

    class Negative( object):
        def __init__( me, operand):
            me.operand = operand
        def __getattr__( me, name):
            return getattr( me.operand, name)

    class sqlite:
        @staticmethod
        def make_sql_template( sql_intersect, sql_select, where4operand, where4negative, where4operand_many =None, where4negative_many =None):
            def template( me, tree):
                union = []
                for ands in tree:
                    intersect = []
                    for op in ands:
                        if isinstance( op, (list, tuple)):
                            neg = isinstance( op[0], QAspect.Negative)
                            ids = tuple([ o.id for o in op])
                            w = (where4negative_many if neg else where4operand_many) % str(ids)
                            intersect.append( sql_intersect % w)
                        else:
                            s = sql_intersect % (where4negative if isinstance(op, QAspect.Negative) else where4operand)
                            intersect.append( s % op.id)
                    union.append( sql_select + ' intersect '.join( intersect) + ')')
                return ' union '.join( union)
            return template

    class Parser( Aspect.Parser):
        class UnknownObj( Aspect.Error):
            def __init__( me, token, expected_type =None):
                me.expected_type = expected_type
                Aspect.Error.__init__( me, token + ' expected:' + me.expected_type.__name__)

        unknownOperand = lambda me, token: me.UnknownObj( token, me.operand.__class__)
        unknownResult = lambda me, token: me.UnknownObj( token, me.result.__class__)

        @property
        def is_taggable( me):
            for t in me.tree:
                if isinstance( t, QAspect.Negative):
                    return False
            return bool(me.result.id) # and os.path.sep not in me.result.name #and len(me.tree) == 1

        def setup_result( me, path):
            pass

        def setup_from_alias( me, obj):
            if obj.__class__ is me.result.__class__:
                me.result = obj
            else:
                me.operand = obj
            return obj

        def _parse( me):
            qgram = me.context.query_grammar
            me.tree = []
            me.complete = False
            tokens = [ t for t in me.path_in_aspect.split( os.path.sep) if t ]
            log('tokens', tokens)
            ands = []
            me.last_token = None
            me.pending_logic = None
            have_negative = False
            for i, token in enumerate(tokens):
                me.last_token = token
                if token == qgram.eval_:
                    if ands:
                        me.tree.append( ands)
                        me.complete = True
                        opath_tokens = tokens[ i+1:] # skip '='
                        if opath_tokens:
                            path_tail = os.path.join( *opath_tokens)
                            me.setup_result( path_tail)
                            if not me.result.id:
                                me.error = me.unknownResult( path_tail)
                    else:
                        me.error = me.unknownOperand( token)
                    break
                elif token == qgram.or_:
                    if not ands or have_negative or me.pending_logic:
                        me.error = me.unknownOperand( token)
                        break
                    me.tree.append( ands)
                    ands = []
                    me.pending_logic = token
                elif token == qgram.and_:
                    if have_negative or me.pending_logic:
                        me.error = me.unknownOperand( token)
                        break
                    me.pending_logic = token
                elif token == qgram.not_:
                    if have_negative:
                        me.error = me.unknownOperand( token)
                        break
                    have_negative = True
                    me.pending_logic = token
                #elif token == qgram.root:
                #    log('ERROR: unexpected symbol for root')
                #    assert 0
                else:
                    logic = me.pending_logic
                    if logic:
                        me.pending_logic = None
                    op = me.token2operand( token)
                    if logic == qgram.not_:
                        op = QAspect.Negative( op)
                    me.operand = op
                    if not op.id:
                        if logic:
                            me.error = me.unknownOperand( token)
                            break
                        if (ands
                                and issubclass( me.op_klas, (Op2, Operations.getattr))
                                and i == len(tokens)-1):
                            me.tree.append( ands)
                            me.setup_result( token)
                            if not me.result.id:
                                me.error = me.unknownResult( token)
                        else:
                            me.error = me.unknownOperand( token) # XXX is it needed?
                        break

                    positive = getattr( op, 'operand', op) # if negative
                    if ands and positive in ands:
                        me.error = me.unknownOperand( token)
                        break
                    if logic in (qgram.not_, qgram.or_) and [ positive ] in me.tree:
                        me.error = me.unknownOperand( token)
                        break
                    ands.append( op)
            else:
                if ands:
                    me.tree.append( ands)

            if me.result.id or getattr( me.error, 'expected_type', None) == me.result.__class__:
                return me.result
            if me.last_token and me.last_token not in qgram.values():# and me.tree:
                return me.operand
            # path ends with something from query_grammar
            return 'query'

        def token2operand( me, token):
            pass

        def tag_tree( me):
            if not me.is_taggable:
                return False
            QAspect.aliases[ me.full_path] = me.result
            ops = set( me.iter_tree( me.tree)) - set( me._get_operands4result( me.result))
            log('tagging', me.result.id, ops)
            me._tag_result( me.result, ops)
            return True

        def iter_tree( me, tree):
            for elem in tree:
                if not isiterable( elem):
                    yield elem
                    continue
                for t in me.iter_tree( elem):
                    yield t

        def untag_tree( me):
            if not me.is_taggable:
                return False

            qops = set( me.iter_tree( me.tree))
            log( 'untagging', me.result.id, qops)
            me._untag_result( me.result, qops)
            return True

        def _get_operands4result( me, result):
            raise NotImplementedError
        def _tag_result( me, result, operands):
            raise NotImplementedError
        def _untag_result( me, result, operands):
            raise NotImplementedError

    ###############

    class getattr( Operations.getattr):
        def query( me, *a):
            if me.q.error:
                log('parse error:', me.q.error)
                return ENOENT
            return me.osexec( me.context.archive)
        operand = negative = query

    class readdir( Operations.readdir):
        def operand( me, o):
            excl = set( me.q.tree[-1])
            names = set([ name for name,op in me._iter_operands() if op not in excl ])
            return me.direntries( me.context.query_grammar.values() + list(names))

        def negative( me, n):
            return me.direntries([ me.context.query_grammar.eval_ ])

        def aspect( me):
            return me.direntries( [ me.context.query_grammar.not_] + [ name for name, op in me._iter_operands() ])

        def query( me):
            q = me.q
            results = dict()
            if q.complete:
                results.update( me._get_results())
                names = results.keys()
                for name,result in results.iteritems():
                    QAspect.aliases[ os.path.join( q.full_path, name)] = result
            else:
                # for convenience exclude tags which are already 'or'ed anyway
                names = [ n for n,o in me._iter_operands() if o not in me._get_single_ors( q)]
                if me.q.last_token != me.context.query_grammar.not_:
                    names.append( me.context.query_grammar.not_)
            log( 'entries:', names)
            return me.direntries( names)

        def _get_single_ors( me, q):
            return set( it.chain( *[ ands for ands in q.tree if len(ands)==1 ] )) # TODO reasoning here?!

    class access( Operations.access):
        operand = result = negative = query = lambda me, *a: None

    class unlink( Operations.unlink):
        def result( me, o):
            me.q.untag_tree()

    class rmdir( Operations.rmdir):
        def negative( me, n):
            return getattr( me, me.context.get_func_name( n.obj))( n.obj)

    class mkdir( Operations.mkdir):
        def negative( me, n):
            return getattr( me, me.context.get_func_name( n.obj))( n.obj)

    class link( Operations.link):
        _on_result_change_name = None

        def result_result( me, fr, to):
            r = None
            if not to.id:
                if fr.name != to.name:
                    if me._on_result_change_name is None:
                        r = ENOSYS
                    else:
                        r = me._on_result_change_name( fr, to)
                else:
                    to.id = fr.id
            me.q2.tag_tree()
            return r

    class symlink( Operations.symlink, link):
        pass

    class rename( Operations.rename):
        def _do_rename( me, fr, to, on_change_name):
            r = None
            if fr.name != to.name:
                r = on_change_name( fr, to)
            if r is None:
                if isinstance( me.q1, QAspect.Parser):
                    me.q1.untag_tree()
                to.id = fr.id
                if isinstance( me.q2, QAspect.Parser):
                    me.q2.tag_tree()
            return r

        def operand_operand( me, fr, to):
            return me._do_rename( fr, to, me._on_operand_change_name)
        def result_result( me, fr, to):
            return me._do_rename( fr, to, me._on_result_change_name)


if 0:
    class Placeholder( Aspect):
        ''' aspect that wraps other aspects in it's directory and does nothing else'''

        @classmethod
        def _init( me, aspects):
            me._rexp = '^/'+me.dir_name()+'$'
            for op_name, op_klas in vars(me).iteritems():
                op_klas = getattr( me, op_name, op_klas)
                if issubclass( op_klas, (Op1,Op2)):
                    class new_op_klas( op_klas):
                        aspects = aspects
                    new_op_klas.__name__ = op_klas.__name__
                    setattr( me, op_name, new_op_klas)

        getattr = Operations.getattr
        access = Operations.access

        class readdir( Operations.readdir):
            def aspect( me):
                return me.direntries([ a.dir_name() for a in me.aspects() ])

        class utime( Operations.utime):
            def aspect( me):
                return me.osexec( me.context.archive, me.times)
        class rename( Operations.rename):
            not_implemented = EPERM
        class symlink( Operations.symlink):
            not_implemented = EPERM
        class link( Operations.link):
            not_implemented = EPERM

# vim:ts=4:sw=4:expandtab
