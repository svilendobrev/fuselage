from tags import Tags
from archive import Archive
#from sql import Schema, Index, Trigger, Table, UsesTables, Constraint
from storage import exec_after
from log import log

class Mintags( Tags):
    class sqlite( Tags.sqlite):
        @exec_after('delete_tag')
        def tag2item_deletetag( me, tag):
            Mintags.tag2items.pop( tag, None)

        @exec_after('tag_item')
        def tag2item_additem( me, item, *tags):
            for tag in tags:
                Mintags.tag2items.setdefault( tag, set()).add( item)

        @exec_after('untag_item')
        def tag2item_removeitem( me, item, *tags):
            empties = set()
            for tag, items in Mintags.tag2items.iteritems():
                items.discard( item)
                if not items:
                    empties.add( tag)
            for tag in empties:
                del Mintags.tag2items[ tag]

        def get_tagging( me):
            return me.query('''
select tagging.tag_id as tag_id, tags.name as tag_name, tagging.item_id as item_id, items.name as item_name
    from tagging
    join tags on tags.id == tagging.tag_id
    join items on items.id == tagging.item_id''')

    tag2items = dict()

    @classmethod
    def init( me, context):
        t2i = me.tag2items
        db = context.db
        for (tag_id, tag_name, item_id, item_name) in db.get_tagging():
            t2i.setdefault( Tags.Tag( id=tag_id, name=tag_name), set()).add( Archive.Item( id=item_id, name=item_name))

    class readdir( Tags.readdir):
        def _iter_operands( me):
            if me.q.path_in_aspect and me.q.pending_logic != me.context.query_grammar.or_:
                qitems = set( me.db.query_items( me.q.tree))
                for tag,items in Mintags.tag2items.iteritems():
                    if items & qitems:
                        yield tag.name, tag
            else:
                for tag in Mintags.tag2items:
                    yield tag.name, tag

# vim:ts=4:sw=4:expandtab
