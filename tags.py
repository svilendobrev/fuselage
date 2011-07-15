from log import log
from aspect import Aspect, id_and_name
from archive import Archive
from operations import Operations, Op2
from sql import Table, Index, RefTables, Schema, Trigger, Constraint
from storage import exec_after
import model
from util.attr import issubclass
from util.struct import DictAttr
from errno import *
import sqlite3
import itertools as it
import os

class Tags( Aspect):
    class Tag( model.NameId):
        pass
    class Negative( object):
        def __init__( me, obj):
            me.obj = obj
        def __getattr__( me, name):
            return getattr( me.obj, name)

    class UnknownTag( Aspect.Error):  pass
    class UnknownItem( Aspect.Error): pass

    class sqlite:
        schema = Schema(
            RefTables( Archive, 'items'),
            tables = dict(
                tags = Table(
                    id      ='integer not null primary key autoincrement',
                    name    ='varchar(65) unique not null',
                ),
                tagging = Table(
                    item_id ='integer not null',
                    tag_id  ='integer not null',
                    tagging_key=Constraint('unique (item_id, tag_id)'),
                ),
            ),
            indexes = dict(tags_index = Index('tagging', 'item_id', 'tag_id')),
        )

        def save_tag( me, tag):
            if not tag.id:
                log('create_tag:', tag)
                me.sql('insert into tags(name) values(?)', tag.name)
                tag.id = me.last_insert_rowid()
            else:
                me.sql('update tags set name=? where id=?', tag.name, tag.id)

        def delete_tag( me, tag):
            me.sql('delete from tags where id = ?', tag.id)
            me.sql('delete from tagging where tag_id = ?', tag.id)

        def get_tags( me, item =None):
            if not item:
                q = me.query('select id, name from tags')
            else:
                q = me.query('''
select tags.id as tag_id, tags.name as tag_name from tags
    join tagging on tags.id == tagging.tag_id
    where tagging.item_id = ?''', item.id)
            return [ Tags.Tag( id=id_, name=name) for id_, name in q ]

        # item and tags
        def tag_item( me, item, *tags):
            me.sql_many('insert into tagging(tag_id, item_id) values(?,?)', [ (tag.id, item.id) for tag in tags ])

        @exec_after('delete_item')
        def untag_item( me, item, *tags):
            if not tags:
                me.sql('delete from tagging where item_id = ?', item.id)
            elif len(tags) == 1:
                tag_id = tags[0].id
                me.sql('delete from tagging where tag_id = ? and item_id = ?', tag_id, item.id)
            else:
                values = [ t.id for t in tags ] + [ item.id ]
                me.sql('delete from tagging where tag_id in (%s) and item_id = ?' % ','.join('?'*len(tags)), *values)

        def get_tagging( me):
            return me.query('''
select tagging.tag_id as tag_id, tags.name as tag_name, tagging.item_id as item_id, items.name as item_name
    from tagging
    join tags on tags.id == tagging.tag_id
    join items on items.id == tagging.item_id''')

        def build_tags_sql( me, tags_tree):
            sql_intersect = '''
select items.id as item_id, items.name as item_name from items
    join tagging on tagging.item_id = items.id
    join tags on tags.id = tagging.tag_id
    where %s'''
            where4tag = 'tags.id = %d'
            where4tag_many = 'tags.id in %s'
            where4negative = 'items.id not in (select item_id from tagging where tag_id = %d)'
            where4negative_many = 'items.id not in (select item_id from tagging where tag_id in %s)'
            sql_select = 'select distinct item_id, item_name from ('
            union = []
            for ands in tags_tree:
                intersect = []
                for tag in ands:
                    if isinstance( tag, (list, tuple)):
                        neg = isinstance( tag[0], Tags.Negative)
                        ids = tuple([ t.id for t in tag ])
                        w = (where4negative_many if neg else where4tag_many) % str(ids)
                        intersect.append( sql_intersect % w)
                    else:
                        s = sql_intersect % (where4negative if isinstance(tag, Tags.Negative) else where4tag)
                        intersect.append( s % tag.id)
                union.append( sql_select + ' intersect '.join( intersect) + ')')
            sql = ' union '.join( union)
            return sql

        def query_items( me, tags_tree):
            return [ Archive.Item( id=id_, name=name) for id_,name in me.query( me.build_tags_sql( tags_tree)) ]

    class Parser( Archive.Parser):
        @property
        def is_taggable( me):
            for t in me.tree:
                if isinstance( t, Tags.Negative):
                    return False
            return me.item.name and os.path.sep not in me.item.name #and len(me.tree) == 1

        def setup_from_path( me, path):
            Archive.Parser.setup_from_path( me, path)
            if not me.item.id:
                items = me.db.query_items( me.tree)
                if me.item not in items:
                    for o in items:
                        if o.name == me.item.name:
                            log('found item:', o)
                            me.item.id = o.id
                            break
                    else:
                        me.item.id = None
                        me.error = Tags.UnknownItem( me.item.name)
                        return
                me.set_item_full_path()

        def setup_from_alias( me, obj):
            me.item = obj
            me.set_item_full_path()
            return obj

        def init( me):
            Archive.Parser.init( me)
            me.tag = Tags.Tag()

        unknownObj = property( lambda( me): Tags.UnknownTag)

        def _parse( me):
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
                if token == me.context.query_grammar.eval_:
                    if ands:
                        me.tree.append( ands)
                        me.complete = True
                        opath_tokens = tokens[ i+1:] # skip '='
                        if opath_tokens:
                            me.setup_from_path( os.path.join( *opath_tokens))
                    else:
                        me.error = me.unknownObj( token)
                    break
                elif token == me.context.query_grammar.or_:
                    if not ands or have_negative or me.pending_logic:
                        me.error = me.unknownObj( token)
                        break
                    me.tree.append( ands)
                    ands = []
                    me.pending_logic = token
                elif token == me.context.query_grammar.and_:
                    if have_negative or me.pending_logic:
                        me.error = me.unknownObj( token)
                        break
                    me.pending_logic = token
                elif token == me.context.query_grammar.not_:
                    if have_negative:
                        me.error = me.unknownObj( token)
                        break
                    have_negative = True
                    me.pending_logic = token
                else:
                    tag = me.handle_token( token)
                    if me.pending_logic:
                        me.pending_logic = None
                        if not tag.id:
                            me.error = me.unknownObj( token)
                            break
                    if not tag.id: # could be an item's name
                        if (ands
                                and issubclass( me.op_klas, (Op2, Operations.getattr))
                                and i == len(tokens)-1):
                            me.tree.append( ands)
                            me.setup_from_path( token)
                        else:
                            me.error = me.unknownObj( token) # XXX is it needed?
                        break
                    ands.append( tag)
            else:
                if ands:
                    me.tree.append( ands)
            return me.result_type()

        def handle_token( me, token):
            me.tag = Tags.name2tag.get( token) or Tags.Tag( name=token)
            if me.pending_logic == me.context.query_grammar.not_:
                me.tag = Tags.Negative( me.tag)
            return me.tag

        def result_type( me):
            if me.item.id or isinstance( me.error, Tags.UnknownItem):
                return me.item
            if me.last_token and me.last_token not in me.context.query_grammar.values():# and me.tree:
                return me.tag
            # path ends with something from query_grammar
            return 'query'

    ##################

    name2tag = dict()

    @classmethod
    def init( me, context):
        name2tag = Tags.name2tag
        for t in context.db.get_tags():
            name2tag[ t.name] = t

    class getattr( Operations.getattr):
        def item( me, o):
            return me.osexec( o.full_path) if o.id else ENOENT

        def tag( me, t):
            return me.osexec( me.context.archive) if t.id else ENOENT

        negative = tag

        def query( me):
            if me.q.error:
                log('parse error:', me.q.error)
                return ENOENT
            return me.osexec( me.context.archive)


    readlink = Archive.readlink
    open = Archive.open

    class access( Archive.access):
        def tag( me, o):
            return None
        negative = tag
        def query( me):
            return None

    class readdir( Archive.readdir):
        def negative( me, n):
            return me.direntries([ me.context.query_grammar.eval_ ])

        def tag( me, t): # negatives not possible here; they can only appear at the end of the query
            excl = set( me.q.tree[-1])
            tags = [ t.name for t in Tags.name2tag.itervalues() if t not in excl ]
            return me.direntries( me.context.query_grammar.values() + tags)

        def _query_tree( me):
            return me.db.query_items( me.q.tree)

        def _add_entry( me, o, entries):
            if o.name in entries:
                olditem = entries.pop( o.name)
                entries[ id_and_name.make( olditem.id, olditem.name)] = olditem
                entries[ id_and_name.make( o.id, o.name)] = o
            else:
                entries[ o.name] = o

        def _iter_all( me):
            for t in Tags.name2tag.itervalues():
                yield t.name, t

        def query( me):
            q = me.q
            items = dict()
            if q.complete:
                for o in me._query_tree():
                    me._add_entry( o, items)
                names = items.keys()
                for name,item in items.iteritems():
                    Tags.aliases[ os.path.join( q.full_path, name)] = item
            else:
                # for convenience exclude tags which are already 'or'ed anyway
                names = [ n for n,o in me._iter_all() if o not in me._get_single_ors( q)]
                if me.q.last_token != me.context.query_grammar.not_:
                    names.append( me.context.query_grammar.not_)
            log( 'entries:', names)
            return me.direntries( names)

        def aspect( me):
            return me.direntries( Tags.name2tag.keys() + [ me.context.query_grammar.not_])

        def _get_single_ors( me, q):
            return set( list(it.chain( *[ ands for ands in q.tree if len(ands)==1 ] )))

    class _tag_untag( object):
        def tag_tree( me, q):
            is_taggable = getattr( q, 'is_taggable', False)
            if is_taggable:
                Tags.aliases[ q.full_path] = q.item
                item_tags = set( me.db.get_tags( q.item))
                qtags = set( it.chain( *q.tree))
                tags = qtags-item_tags
                log( 'tagging', q.item.id, tags)
                me.db.tag_item( q.item, *tags)
                #item_tags.update( tags)
            return is_taggable

        def untag_tree( me, q):
            is_taggable = getattr( q, 'is_taggable', False)
            if is_taggable:
                qtags = set( it.chain( *q.tree))
                log( 'untagging', q.item.id, qtags)
                me.db.untag_item( q.item, *qtags)
            return is_taggable

    class unlink( Archive.unlink, _tag_untag):
        def item( me, o):
            # XXX do we have to delete an item which is no longer tagged ??
            if not me.untag_tree( me.q):
                return Archive.unlink.item( me, o)

    class rmdir( Archive.rmdir, _tag_untag):
        def item( me, o):
            if not me.untag_tree( me.q):
                return Archive.rmdir.item( me, o)
        def tag( me, t):
            me.db.delete_tag( t)
            Tags.name2tag.pop( t.name)
        def negative( me, n):
            return me.tag( n.obj)

    class mkdir( Archive.mkdir, _tag_untag):
        not_implemented = EFAULT

        def item( me, o):
            me.tag_tree( me.q)
            return Archive.mkdir.item( me, 0)

        def tag( me, t):
            try:
                me.db.save_tag( t)
                Tags.name2tag[ t.name] = t
            except sqlite3.DatabaseError, e:
                log('error:', e)
                return EFAULT

        def negative( me, n):
            return me.tag( n.obj)

    class link( Archive.link, _tag_untag):
        def item_item( me, fr, to):
            q1,q2 = me.q1, me.q2
            if not to.id and fr.name == to.name:
                to.id = fr.id
                q2.set_item_full_path()
            if not to.id:
                Archive.link.item_item( me, fr, to)
            is_taggable = getattr( me.q2, 'is_taggable', False)
            if is_taggable:
                me.tag_tree( me.q2)

        def external_item( me, ext, item):
            q2 = me.q2
            is_taggable = getattr( q2, 'is_taggable', False)
            if is_taggable:
                r = Archive.link.external_item( me, ext, item)
                me.tag_tree( q2)
                return r

    class symlink( Operations.symlink, link):
        pass

    class rename( Archive.rename, _tag_untag):
        def item_item( me, fr, to):
            # what to do here ????
            # options for untagging the left side:
            #  - always do nothing
            #  - untag all the tags on the left side
            #  - untag only the first group of tags in the tree (up to the first "or")
            #  - do nothing in case there are "or"s in the query, otherwise untag
            # similarly for the right side:
            #  - tag with all the tags on the left side regardless of "or" clauses
            #  - tag only with the first/last/middle/odd/even/random group of and'ed tags in the tree
            #  - do nothing in case there are "or"s in the query, otherwise tag

            # for now we untag with all present tags in the tree
            if not to.id and not isinstance( me.q2.error, Tags.UnknownItem):
                return Archive.rename.item_item( me, fr, to)
            is_taggable1 = getattr( me.q1, 'is_taggable', False)
            if is_taggable1:
                me.untag_tree( me.q1)
            to.id = fr.id
            me.q2.set_item_full_path()
            me.tag_tree( me.q2)

        #def item_tag( me): ???

        def tag_tag( me, fr, to):
            to.id = fr.id
            me.db.save_tag( to)
            del Tags.name2tag[ fr.name]
            Tags.name2tag[ to.name] = to

        # renaming negated tags is not supported

    class chmod( Archive.chmod):
        def tag( me, t):
            return None
        negative = tag

# vim:ts=4:sw=4:expandtab
