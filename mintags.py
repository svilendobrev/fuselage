from log import log
from tags import Tags, Operations
from archive import Archive
from sql import Schema, Index, Trigger, Table, RefTables, Constraint
from storage import exec_after
from util.struct import DictAttr
import itertools as it

class Mintags( Tags):
    class sqlite( Tags.sqlite):
        @exec_after('delete_tag')
        def tag2item_deletetag( me, tag):
            Mintags.tag2items.pop( tag, None)
            #me.sql('delete from tags_assoc where tag1_id=? or tag2_id=?', tag_id, tag_id)

        @exec_after('tag_item')
        def tag2item_additem( me, item, *tags):
            for tag in tags:
                Mintags.tag2items.setdefault( tag, set()).add( item)

        @exec_after('untag_item')
        def tag2item_removeitem( me, item, *tags):
            empties = set()
            if not tags:
                for tag, items in Mintags.tag2items.iteritems():
                    items.discard( item)
                    if not items:
                        empties.add( tag)
            else:
                for tag in tags:
                    items = Mintags.tag2items.get( tag)
                    if items is None: continue
                    items.discard( item)
                    if not items:
                        empties.add( tag.id)
            for tag in empties:
                del Mintags.tag2items[ tag]

    tag2items = dict()

    @classmethod
    def init( me, context):
        t2i = me.tag2items
        db = context.db
        for (tag_id, tag_name, item_id, item_name) in db.get_tagging():
            t2i.setdefault( Tags.Tag( id=tag_id, name=tag_name), set()).add( Archive.Item( id=item_id, name=item_name))

    class readdir( Tags.readdir):
        def tag( me, t):
            excl = set( me.q.tree[-1])
            qitems = set( me.db.query_items( me.q.tree))
            names = [ tag.name
                      for (tag, items) in Mintags.tag2items.iteritems()
                      if tag not in excl and items & qitems ]
            return me.direntries( list(Tags.signs) + names)

        def query( me):
            q = me.q
            if q.complete:
                return Tags.readdir.query( me)
            single_ors = me._get_single_ors( q)
            log( 'single_ors:', single_ors)
            names = [ tag.name for tag in Mintags.tag2items if tag not in single_ors ]
            log( 'entries:', names)
            return me.direntries( names)

        def aspect( me):
            return me.direntries([ tag.name for tag in Mintags.tag2items ])


# vim:ts=4:sw=4:expandtab
