from tags import Tags
from aspect import Aspect
from operations import Operations
from sql import RefTables, Constraint, Table, Schema, Index
from storage import exec_after, exec_before
import itertools as it
from util.struct import DictAttr
from log import log
import os

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

    def set( me, rel):
        getattr( me, '_add_'+rel.typ)( rel.ltag, rel.rtag)
    def unset( me, rel):
        getattr( me, '_del_'+rel.typ)( rel.ltag, rel.rtag)

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


class Relations( Tags):
    #_dir_name = 'rels'

    links = None

    class sqlite:
        schema = Schema(
            RefTables( Tags, 'tags'),
            tables = dict(
                inctags = Table(
                    ltag ='integer not null',
                    rtag ='integer not null',
                    inctags_key= Constraint('unique (ltag, rtag)'),  #or compose-primary-key?
                ),
            ),
            indexes = dict( inctags_index = Index('inctags', 'ltag', 'rtag')),
        )
        # store only original links ; infer in-memory the consequences

        @exec_before('query_items')
        def modify_tree4reasoning( me, tags_tree):
            new_tree = []
            for ands in tags_tree:
                new_ands = []
                for tag in ands:
                    neg = None
                    if isinstance( tag, Relations.Negative):
                        neg = tag
                        tag = neg.obj
                    alt_tags = list(Relations.links.query( tag, IS)) + list(Relations.links.query( tag, HAS))
                    if alt_tags:
                        if neg:
                            alt_tags = [ Relations.Negative(t) for t in alt_tags ]
                        new_ands.append( [ neg or tag ] + alt_tags)
                    else:
                        new_ands.append( neg or tag)
                new_tree.append( new_ands)
            tags_tree[:] = new_tree # modify param for query_items

        @exec_after('delete_tag')
        def on_delete_tag( me, tag):
            me.sql('delete from inctags where ltag=? or rtag=?', tag.id, tag.id)
            Relations.links.delete_tag( tag)

        def save_rel( me, r):
            log('save_rel:', r)
            ltag, rtag = r.ltag, r.rtag
            me.sql('delete from inctags where (ltag=? and rtag=?) or (ltag=? and rtag=?)', ltag.id, rtag.id, rtag.id, ltag.id)
            eq = r.typ == 'is'
            if eq or r.typ == 'has':
                me.sql('insert into inctags( ltag, rtag) values(?,?)', ltag.id, rtag.id)
            if eq or r.typ == 'in':
                me.sql( 'insert into inctags( ltag, rtag) values(?,?)', rtag.id, ltag.id)
            Relations.links.set( r)

        def delete_rel( me, r):
            log('delete_rel:', r)
            eq = r.typ == 'is'
            ltag, rtag = r.ltag, r.rtag
            if eq or r.typ == 'has':
                me.sql('delete from inctags where ltag=? and rtag=?', ltag.id, rtag.id)
            if eq or r.typ == 'in':
                me.sql('delete from inctags where ltag=? and rtag=?', rtag.id, ltag.id)
            Relations.links.unset( r)

        def get_all_rels( me):
            return [ (Relations.name2tag[ lname], Relations.name2tag[ rname])
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

    class Rel( object):
        ltag = None
        rtag = None
        typ = None

        def __hash__( me):
            lid = getattr( me.ltag, 'id', None)
            rid = getattr( me.rtag, 'id', None)
            return hash( '-'.join([ str(s) for s in lid,me.typ,rid ]))

        def __eq__( me, o):
            if o is None:
                return False
            return me.ltag == o.ltag and me.rtag == o.ltag and me.typ == o.typ


    class UnknownRelType( Aspect.Error):  pass
    class UnrelatedTag( Aspect.Error):  pass

    class Parser( Aspect.Parser):
        def init( me):
            Aspect.Parser.init( me)
            me.rel = Relations.Rel()

        def _parse( me):
            tokens = [ t for t in me.path_in_aspect.split( os.path.sep) if t ]
            log('tokens', tokens)
            ltokens = len( tokens)
            if ltokens > 3:
                return 'error'
            for i,t in enumerate(tokens):
                if i == 0:
                    if me._set_ltag( t):
                        break
                elif i == 1:
                    if t not in reltypes:
                        me.error = Relations.UnknownRelType( t)
                        return 'error'
                    me.rel.typ = t
                elif i == 2:
                    me._set_rtag( t)
            return me.result_type()

        def result_type( me):
            if me.rel.rtag:
                return me.rel
            if me.rel.typ:
                return 'query'
            return me.rel.ltag

        def _set_ltag( me, t):
            me.rel.ltag = Relations.name2tag.get( t, Tags.Tag( name=t))
            if not me.rel.ltag.id:
                me.error = Tags.UnknownTag( t)
                return True

        def _set_rtag( me, t):
            me.rel.rtag = Relations.name2tag.get( t, Tags.Tag( name=t))
            if not me.rel.rtag.id:
                me.error = Tags.UnknownTag( t)
            elif me.rel.rtag not in Relations.links.query( me.rel.ltag, me.rel.typ):
                me.error = Relations.UnrelatedTag( t)


    @classmethod
    def init( me, context):
        if me.links is None:
            if Tags not in context.aspects:
                Tags.init( context)
            me.links = Links( context.db.get_all_rels())

    class getattr( Tags.getattr):
        def rel( me, o):
            return me.as_file( Tags.getattr.query( me))

    class readdir( Operations.readdir):
        def tag( me, t):
            return me.direntries( reltypes)

        def aspect( me):
            return me.direntries( Relations.name2tag)

        def query( me):
            return me.direntries([ t.name for t in Relations.links.query( me.q.rel.ltag, me.q.rel.typ) ])

    class unlink( Operations.unlink):
        def rel( me, r):
            me.db.delete_rel( r)

    class link( Operations.link):
        def rel_rel( me, rfrom, rto):
            if rfrom.rtag.name == rto.rtag.name:
                me.db.save_rel( rto)
            else: #???
                pass

    class rename( Operations.rename):
        def rel_rel( me, rfrom, rto):
            if not rto.rtag.id:
                return EINVAL

            if isinstance( me.q2.error, Relations.UnrelatedTag):
                me.db.save_rel( rto)
            me.db.delete_rel( rfrom)

        def tag_rel( me, tag, rel):
            if not rel.rtag.id:
                return EINVAL
            me.db.save_rel( rel)

    class chmod( Tags.chmod):
        query = rel = Tags.chmod.tag


class ParentTags( Relations):
    _dir_name = 'parent_tags'

    class Parser( Relations.Parser):
        rel_type = IN
        def _parse( me):
            tokens = [ t for t in me.path_in_aspect.split( os.path.sep) if t ]
            log('tokens', tokens)
            ltokens = len( tokens)
            if ltokens > 2: return 'error'
            if ltokens:
                me.rel.typ = me.rel_type
                me._set_ltag( tokens[0])
            if ltokens > 1:
                me._set_rtag( tokens[1])
            return me.result_type()

    class rename( Relations.rename):
        def query_rel( me, rel):
            return me.tag_rel( me.q1.rel.ltag, rel)

class ChildTags( ParentTags):
    _dir_name = 'child_tags'
    class Parser( ParentTags.Parser):
        rel_type = HAS

class EqTags( ParentTags):
    _dir_name = 'eq_tags'
    class Parser( ParentTags.Parser):
        rel_type = IS


# vim:ts=4:sw=4:expandtab
