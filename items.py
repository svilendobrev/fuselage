from tags import Tags
from aspect import id_and_name
from archive import Archive
from operations import Operations
from sql import RefTables, Schema
from errno import *
import sqlite3
from log import log
import itertools as it
import os

class Items( Tags):
    class sqlite:
        def build_items_sql( me, items_tree):
            s = me.build_tags_sql( items_tree)
            s = s.replace('tagging', 'Tagging')
            s = s.replace('item', 'Tag')
            s = s.replace('tag', 'Item')
            return s

        def query_tags( me, items_tree):
            return [ Items.Tag( id=id_, name=name) for id_,name in me.query( me.build_items_sql( items_tree)) ]

    class Parser( Tags.Parser):
        def setup_from_path( me, path):
            if not me.tag.id:
                me.check4alias()
            if not me.tag.id:
                tagname = path.split( os.path.sep)[-1]
                tags = set( me.db.query_tags( me.tree))
                me.tag = Items.name2tag.get( tagname, me.tag)
                me.error = Items.UnknownTag( me.tag.name) if me.tag not in tags else None

        def setup_from_alias( me, obj):
            if isinstance( obj, Archive.Item):
                me.item = obj
                me.set_item_full_path()
            else:
                me.tag = obj
            return obj

        unknownObj = property( lambda me: Items.UnknownItem)

        def handle_token( me, token):
            d = id_and_name
            me.item = me.db.first( me.db.get_item( id_=d.get_id( token), name=d.get_name( token)))
            me.item = me.item or Archive.Item( name=token)
            if me.item.id:
                me.set_item_full_path()
            if me.pending_logic == me.context.query_grammar.not_:
                me.item = Items.Negative( me.item)
            return me.item

        def result_type( me):
            if me.tag.id or isinstance( me.error, Items.UnknownTag):
                return me.tag
            if me.last_token and me.last_token not in me.context.query_grammar.values():
                return me.item
            # path ends with '=' or '+'
            return 'query'

    @classmethod
    def init( me, context):
        if Tags not in context.aspects:
            Tags.init( context)

    class getattr( Tags.getattr):
        def item( me, o):
            return me.as_dir( Tags.getattr.item( me, o))

        def tag( me, t):
            return me.query()

    class access( Tags.access):
        def item( me, o):
            return None if o.id else EACCES

    class readdir( Tags.readdir):
        def tag( me, t):
            for name in me.context.query_grammar.values():
                Items.aliases[ os.path.join( me.q.full_path, name)] = t
            return me.direntries( me.context.query_grammar.values())

        def _query_tree( me):
            return me.db.query_tags( me.q.tree)

        def _add_entry( me, o, entries):
            entries[ o.name] = o

        def _iter_all( me):
            for i in me.db.get_item():
                yield id_and_name.make(i.id, i.name), i

        def aspect( me):
            return me.direntries( [ me.context.query_grammar.not_] + me._item_names())

        def item( me, o):
            excl = set( me.q.tree[-1])
            return me.direntries( me.context.query_grammar.values() + me._item_names( excl))

    class unlink( Operations.unlink):
        def tag( me, t):
            me.db.untag_item( me.q.item, t)

    rmdir = Archive.rmdir
    unlink = None # everything is a directory here

    class _tag_untag( Tags._tag_untag):
        def tag_tree( me, q):
            is_taggable = getattr( q, 'is_taggable', False)
            if is_taggable:
                Items.aliases[ q.full_path] = q.tag
                tag_items = set( me.db.query_items( [[ q.tag ]]))
                qitems = set( it.chain( *q.tree))
                items = qitems - tag_items
                log( 'tagging', q.tag.id, items)
                for i in items:
                    me.db.tag_item( i, q.tag)
                tag_items.update( items)
            return is_taggable

        def untag_tree( me, q):
            is_taggable = getattr( q, 'is_taggable', False)
            if is_taggable:
                qitems = set( it.chain( *q.tree))
                log( 'untagging', q.tag.id, qitems)
                for i in qitems:
                    me.db.untag_item( i, q.tag)
            return is_taggable

    class rmdir( _tag_untag, Tags.rmdir, Archive.rmdir, Archive.unlink):
        def item( me, o):
            try:
                return Archive.unlink.item( me, o)
            except OSError:
                return Archive.rmdir.item( me, o)

        def tag( me, t):
            me.untag_tree( me.q)

    # mkdir is not allowed
    class mkdir( Operations.mkdir):
        not_implemented = EFAULT
    # symlinks are not allowed
    class symlink( Operations.symlink):
        pass
    # links are not possible as everything is a directory here
    class link( Operations.link):
        pass

    class rename( _tag_untag, Tags.rename):
        item_item = Archive.rename.item_item.im_func
        def tag_tag( me, fr, to):
            if me.q1.tree == me.q2.tree and not to.id:
                try:
                    return Tags.rename.tag_tag( me, fr, to)
                except sqlite3.DatabaseError, e:
                    log('error:', e)
                    return EFAULT
            if isinstance( me.q1, Items.Parser):
                me.untag_tree( me.q1)
            if isinstance( me.q2, Items.Parser) and to.id:
                me.tag_tree( me.q2)


class Untagged( Archive):
    class sqlite:
        schema = Schema(
            RefTables( Archive, 'items'),
            RefTables( Tags, 'tagging'),
        )
        def get_untagged( me):
            return me.query('''
select id, name from items
    where not exists (select * from tagging where tagging.item_id == items.id)
            ''')

    class Parser( Archive.Parser):
        def check4alias( me):
            pass

        def _parse( me):
            Archive.Parser._parse( me)
            if not me.path_in_aspect:
                return 'query'
            untagged = set([ item_id for item_id, name in me.db.get_untagged() ])
            if me.item.id not in untagged:
                me.error = Tags.UnknownItem( me.item.full_path)
            return me.item

    class getattr( Archive.getattr):
        def item( me, o):
            return Archive.getattr.item( me, o) if not me.q.error else ENOENT

    class readdir( Archive.readdir):
        def aspect( me):
            return me.direntries([ id_and_name.make( id_, name) for id_,name in me.db.get_untagged() ])


# vim:ts=4:sw=4:expandtab
