from tags import Tags
from aspect import QAspect, id_and_name
from archive import Archive
from operations import Operations
from sql import UsesTables, Schema
from errno import *
import sqlite3
from log import log
import os

class Items( QAspect):
    operandType = Archive.Item
    resultType = Tags.Tag

    class sqlite:
        schema = Schema(
            UsesTables( Archive, 'items'),
            UsesTables( Tags, 'tags', 'tagging'),
        )
        def build_items_sql( me, items_tree):
            s = me.build_tags_sql( items_tree)
            s = s.replace('tagging', 'Tagging')
            s = s.replace('item', 'Tag')
            s = s.replace('tag', 'Item')
            return s

        def query_by_items( me, items_tree):
            return [ Tags.Tag( id=id_, name=name) for id_,name in me.query( me.build_items_sql( items_tree)) ]

        def get_tag_by_name( me, name):
            d = me.first( me.query('select id, name from tags where name=?', name))
            if d:
                return Tags.Tag( id=d[0], name=d[1])
            return None

    class Parser( QAspect.Parser):
        def init( me):
            me.item = Archive.Item()
            me.tag = Tags.Tag()

        def setup_result( me, path):
            me.tag = me.db.get_tag_by_name( path) or Tags.Tag( name=path)
            if me.tag.id:
                tags = set( me.db.query_by_items( me.tree))
                if me.tag not in tags:
                    me.tag.id = None

        def token2operand( me, token):
            d = id_and_name
            o = me.db.first( me.db.get_item( id_=d.get_id( token), name=d.get_name( token)))
            if not o:
                o = Archive.Item( name=token)
            o.set_full_path( me.context)
            return o

        def _get_operands4result( me, t):
            return me.db.query_by_tags( [[ t ]])
        def _tag_result( me, tag, items):
            for i in items:
                me.db.tag_item( i, tag)
        def _untag_result( me, tag, items):
            for i in items:
                me.db.untag_item( i, tag)

    ##################

    @classmethod
    def init( me, context):
        if Tags not in context.aspects:
            Tags.init( context)

    class getattr( QAspect.getattr):
        def tag( me, t):
            return me.as_file( me.query())

    class readdir( QAspect.readdir):
        def _get_results( me):
            d = {}
            for o in me.db.query_by_items( me.q.tree):
                d[ o.name] = o
            return d

        def _iter_operands( me):
            items = {}
            for o in me.db.get_item():
                if o.name in items:
                    olditem = items.pop( o.name)
                    items[ id_and_name.make( olditem.id, olditem.name)] = olditem
                    items[ id_and_name.make( o.id, o.name)] = o
                else:
                    items[ o.name] = o
            for name,item in items.iteritems():
                Items.aliases[ os.path.join( me.q.full_path, name)] = item
                yield name, item

    class rmdir( QAspect.rmdir, Archive.unlink):
        def item( me, o):
            return Archive.unlink.item( me, o)

        def tag( me, o):
            me.q.untag_tree()

    # mkdir is not allowed
    class mkdir( Operations.mkdir):
        not_implemented = EFAULT

    class rename( QAspect.rename):
        _on_result_change_name = Tags.rename.tag_tag.im_func
        _on_operand_change_name = Archive.rename.item_item.im_func


class Untagged( Archive):
    class sqlite:
        schema = Schema(
            UsesTables( Archive, 'items'),
            UsesTables( Tags, 'tagging'),
        )
        def get_untagged( me):
            q = me.query('''
select id, name from items
    where not exists (select * from tagging where tagging.item_id == items.id)
            ''')
            return [ Archive.Item( id=_id, name=name) for _id, name in q ]

    class Parser( Archive.Parser):
        def _parse( me):
            me.setup_from_path( me.path_in_aspect)
            untagged = set( me.db.get_untagged())
            if me.item not in untagged:
                me.item.name = os.path.split( me.path_in_aspect)[1]
                me.item.id = None
            me.item.set_full_path( me.context)
            return me.item

    class readdir( Archive.readdir):
        def _get_items( me):
            return me.db.get_untagged()

# vim:ts=4:sw=4:expandtab
