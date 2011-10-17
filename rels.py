from tags import Tags
from items import Items
from aspect import QAspect
from operations import Operations
from sql import UsesTables, Constraint, Table, Schema, Index
from storage import exec_after, exec_before
import itertools as it
from util.struct import DictAttr
from log import log

HAS = 'has'
IN = 'in'
IS = 'is'
reltypes = HAS, IN, IS

from kjbuckets import kjGraph

class Links( object):
    def __init__( me, pairs):
        me.graph = kjGraph( pairs)
        me.links = DictAttr()
        me.tag_has = me.links.setdefault( HAS, {})
        me.tag_is = me.links.setdefault( IS, {})
        me.tag_in = me.links.setdefault( IN, {})
        me.recalc()

    def recalc( me):
        tgraph = me.graph.tclosure()
        me.tag_has.clear()
        me.tag_is.clear()
        me.tag_in.clear()
        done = set()
        for ltag, rtag in tgraph.items():
            if ltag == rtag: continue
            if (ltag, rtag) in done: continue
            if tgraph.member( rtag, ltag):
                me._add( me.tag_is, ltag, rtag)
                me._add( me.tag_is, rtag, ltag)
                done.add((rtag, ltag))
                continue
            me._add( me.tag_has, ltag, rtag)
            me._add( me.tag_in, rtag, ltag)
            done.add( (ltag, rtag) )

    def query( me, tag, typ):
        return me.links[ typ].get( tag, set())

    def set( me, ltag, rtag, typ):
        getattr( me, '_add_'+typ)( ltag, rtag)
    def unset( me, ltag, rtag, typ):
        getattr( me, '_del_'+typ)( ltag, rtag)

    def delete_tag( me, tag):
        items = me.graph.items()
        g = me.graph
        for pair in items:
            if tag in pair:
                g.delete_arc( *pair)
        me.recalc()

    def _add( me, dct, ltag, rtag):
        dct.setdefault( ltag, set()).add( rtag)

    def _add_has( me, ltag, rtag):
        me.graph.add( ltag, rtag)
        me._del_has( rtag, ltag)
    def _del_has( me, ltag, rtag):
        if me.graph.member( ltag, rtag):
            me.graph.delete_arc( ltag, rtag)
        me.recalc()

    def _add_in( me, ltag, rtag):
        me._add_has( rtag, ltag)
    def _del_in( me, ltag, rtag):
        me._del_has( rtag, ltag)

    def _add_is( me, ltag, rtag):
        me.graph.add( ltag, rtag)
        me.graph.add( rtag, ltag)
        me.recalc()
    def _del_is( me, ltag, rtag):
        me.graph.delete_arc( ltag, rtag)
        me.graph.delete_arc( rtag, ltag)
        me.recalc()


class RelationsBase( QAspect):
    class Op( Tags.Tag): pass
    operandType = Op
    resultType = Tags.Tag

    links = None

    class sqlite:
        schema = Schema(
            UsesTables( Tags, 'tags'),
            tables = dict(
                inctags = Table(
                    ltag ='integer not null',
                    rtag ='integer not null',
                    inctags_key= Constraint('unique (ltag, rtag)'),  #or compose-primary-key?
                ),
            ),
            indexes = dict( inctags_index = Index('inctags', 'ltag', 'rtag')),
        )
        # store only primary links ; infer in-memory the consequences

        @exec_before('query_by_tags')
        def reason_tree( me, tree):
            new_tree = []
            for ands in tree:
                new_ands = []
                for tag in ands:
                    neg = None
                    if isinstance( tag, QAspect.Negative):
                        neg = tag
                        tag = neg.operand
                    alt_tags = list(RelationsBase.links.query( tag, IS)) + list(RelationsBase.links.query( tag, HAS))
                    if alt_tags:
                        if neg:
                            alt_tags = [ QAspect.Negative(t) for t in alt_tags ]
                        new_ands.append( [ neg or tag ] + alt_tags)
                    else:
                        new_ands.append( neg or tag)
                new_tree.append( new_ands)
            tree[:] = new_tree # modify param for query_by_tags

        @exec_after('delete_tag')
        def on_delete_tag( me, tag):
            me.sql('delete from inctags where ltag=? or rtag=?', tag.id, tag.id)
            RelationsBase.links.delete_tag( tag)

        def save_rel( me, ltag, rtag, typ):
            log('save_rel:', ltag, rtag, typ)
            me.sql('delete from inctags where (ltag=? and rtag=?) or (ltag=? and rtag=?)', ltag.id, rtag.id, rtag.id, ltag.id)
            eq = typ == 'is'
            if eq or typ == 'has':
                me.sql('insert into inctags( ltag, rtag) values(?,?)', ltag.id, rtag.id)
            if eq or typ == 'in':
                me.sql( 'insert into inctags( ltag, rtag) values(?,?)', rtag.id, ltag.id)
            RelationsBase.links.set( ltag, rtag, typ)

        def delete_rel( me, ltag, rtag, typ):
            log('delete_rel:', ltag, rtag, typ)
            eq = typ == 'is'
            if eq or typ == 'has':
                me.sql('delete from inctags where ltag=? and rtag=?', ltag.id, rtag.id)
            if eq or typ == 'in':
                me.sql('delete from inctags where ltag=? and rtag=?', rtag.id, ltag.id)
            RelationsBase.links.unset( ltag, rtag, typ)

        def get_all_rels( me):
            return [ (Tags.Tag(id=lid, name=lname), Tags.Tag(id=rid, name=rname))
                    for lid, lname, rid, rname in me.query('''
select inctags.ltag as leftid, ltags.name as lname, inctags.rtag as rightid, rtags.name as rname
    from inctags
    join tags as ltags on ltags.id == inctags.ltag
    join tags as rtags on rtags.id == inctags.rtag''') ]


    #md /inctags/a/b -> a inc b
    #ls /inctags/ : a/b
    #ls /inctags/a/b : empty

    #md /inctags/a/d -> a inc d
    #ls /inctags/ : a/b a/d
    #TODO reverse: ls /parenttags/b : b/a

    #md /inctags/a/b/c : error ; too dubious a/b b/c ..

    #rd /inctags/a/b -> a notinc b
    #rd /inctags/a/b/c -> error
    #rm === rd
    #maybe show level 2 (b of a/b) as files?

    class Parser( QAspect.Parser):
        REL_TYPE = None # has, is, in
        def init( me):
            me.op = RelationsBase.Op()
            me.tag = Tags.Tag()

        def setup_result( me, path):
            me.tag = me.db.get_tag_by_name( path) or Tags.Tag( name=path)
            if me.tag.id:
                tags = set( me.get_results())
                if me.tag not in tags:
                    me.tag = Tags.Tag( name=path)

        def get_results( me):
            res = set()
            for ands in me.tree:
                s = None
                for op in ands:
                    tags = RelationsBase.links.query( op, me.REL_TYPE)
                    s = tags if s is None else s.intersection( tags)
                res.update( s)
            return res

        def token2operand( me, token):
            o = me.db.get_tag_by_name( token)
            if o:
                o = RelationsBase.Op( id=o.id, name=o.name)
            else:
                o = RelationsBase.Op( name=token)
            return o

        def _get_operands4result( me, tag):
            raise NotImplementedError
        def _tag_result( me, result, operands):
            for o in operands:
                me.db.save_rel( RelationsBase.resultType(id=o.id, name=o.name), result, me.REL_TYPE)
        def _untag_result( me, result, operands):
            for o in operands:
                me.db.delete_rel( o, result, me.REL_TYPE)

    @classmethod
    def init( me, context):
        if RelationsBase.links is None:
            RelationsBase.links = Links( context.db.get_all_rels())

    class getattr( Items.getattr):
        pass

    class readdir( QAspect.readdir):
        def _get_results( me):
            d = {}
            for o in me.q.get_results():
                d[ o.name] = o
            return d

        def _iter_operands( me):
            for t in me.db.get_all_tags():
                yield t.name, t

    class rmdir( QAspect.rmdir):
        operand = Tags.rmdir.tag.im_func

    class mkdir( QAspect.mkdir):
        operand = Tags.rmdir.tag.im_func

    class rename( QAspect.rename):
        _on_result_change_name = _on_operand_change_name = Tags.rename.tag_tag.im_func

        def op_tag( me, op, tag): # unique for this aspect because operandType == resultType
            me.q2.result.id = op.id
            me.q2.tag_tree()

class In( RelationsBase):
    _dir_name = 'parentOf'
    class Parser( RelationsBase.Parser):
        REL_TYPE = IN

        def _get_operands4result( me, tag):
            return RelationsBase.links.tag_has.get( tag, set())

class Has( RelationsBase):
    _dir_name = 'childrenOf'
    class Parser( RelationsBase.Parser):
        REL_TYPE = HAS
        def _get_operands4result( me, tag):
            return RelationsBase.links.tag_in.get( tag, set())

class Is( RelationsBase):
    _dir_name = 'sameAs'
    class Parser( RelationsBase.Parser):
        REL_TYPE = IS
        def _get_operands4result( me, tag):
            return RelationsBase.links.tag_is.get( tag, set())

if 0:
    class Rels( Placeholder):
        @classmethod
        def init( me, context):
            me._init( (Has, In, Is))


# vim:ts=4:sw=4:expandtab
