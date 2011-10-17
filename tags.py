from aspect import QAspect, id_and_name
from archive import Archive
from operations import Operations
from sql import Table, Index, UsesTables, Schema, Trigger, Constraint
from storage import exec_after
import model
from log import log
from errno import *
import sqlite3

class Tags( QAspect):
    class Tag( model.NameId): pass

    operandType = Tag
    resultType = Archive.Item

    class sqlite:
        schema = Schema(
            UsesTables( Archive, 'items'),
            tables = dict(
                tags = Table(
                    id      ='integer not null primary key autoincrement',
                    name    ='varchar(65) unique not null',
                    tags_key=Constraint('unique (name)'),
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
            Tags.name2tag[ tag.name] = tag

        def delete_tag( me, tag):
            me.sql('delete from tags where id = ?', tag.id)
            me.sql('delete from tagging where tag_id = ?', tag.id)
            del Tags.name2tag[ tag.name]

        def get_all_tags( me):
            q = me.query('select id, name from tags')
            return [ Tags.Tag( id=id_, name=name) for id_, name in q ]

        # item and tags
        def get_item_tags( me, item):
            q = me.query('''
select tags.id as tag_id, tags.name as tag_name from tags
    join tagging on tags.id == tagging.tag_id
    where tagging.item_id = ?''', item.id)
            return [ Tags.Tag( id=id_, name=name) for id_, name in q ]

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

        build_tags_sql = QAspect.sqlite.make_sql_template(
            sql_intersect = '''
select items.id as item_id, items.name as item_name from items
    join tagging on tagging.item_id = items.id
    join tags on tags.id = tagging.tag_id
    where %s'''
            , sql_select = 'select distinct item_id, item_name from ('
            , where4operand  = 'tags.id = %d'
            , where4negative = 'items.id not in (select item_id from tagging where tag_id = %d)'
            , where4operand_many = 'tags.id in %s'
            , where4negative_many = 'items.id not in (select item_id from tagging where tag_id in %s)'
        )
        def query_by_tags( me, tags_tree):
            return [ Archive.Item( id=id_, name=name) for id_,name in me.query( me.build_tags_sql( tags_tree)) ]


    class Parser( QAspect.Parser, Archive.Parser):
        def init( me):
            me.item = Archive.Item()
            me.tag = Tags.Tag()

        def setup_result( me, path):
            Archive.Parser.setup_from_path( me, path)
            items = me.db.query_by_tags( me.tree)
            if me.item.id:
                if me.item not in items:
                    me.item.id = None
            else:
                for o in items:
                    if o.name == me.item.name:
                        log('found item:', o)
                        me.item.id = o.id
                        break
            me.item.set_full_path( me.context)

        def token2operand( me, token):
            return Tags.name2tag.get( token) or Tags.Tag( name=token)

        def _get_operands4result( me, item):
            return me.db.get_item_tags( item)
        def _tag_result( me, item, tags):
            me.result.set_full_path( me.context)
            me.db.tag_item( item, *tags)
        def _untag_result( me, item, tags):
            me.result.set_full_path( me.context)
            me.db.untag_item( item, *tags)


    ##################

    name2tag = dict()

    @classmethod
    def init( me, context):
        name2tag = Tags.name2tag
        for t in context.db.get_all_tags():
            name2tag[ t.name] = t

    class getattr( QAspect.getattr):
        item = Archive.getattr.item.im_func

    readlink = Archive.readlink
    open = Archive.open

    class readdir( QAspect.readdir):
        def _get_results( me):
            d = {}
            for o in me.db.query_by_tags( me.q.tree):
                o.set_full_path( me.context)
                if o.name in d:
                    olditem = d.pop( o.name)
                    d[ id_and_name.make( olditem.id, olditem.name)] = olditem
                    d[ id_and_name.make( o.id, o.name)] = o
                else:
                    d[ o.name] = o
            return d

        def _iter_operands( me):
            for t in Tags.name2tag.itervalues():
                yield t.name, t

    class rmdir( QAspect.rmdir):
        def tag( me, t):
            me.db.delete_tag( t)

    class mkdir( QAspect.mkdir):
        def tag( me, t):
            try:
                me.db.save_tag( t)
            except sqlite3.DatabaseError, e:
                log('error:', e)
                return EFAULT

    class link( QAspect.link, Archive.link):
        _on_result_change_name = Archive.link.item_item.im_func

        def external_item( me, ext, item):
            r = Archive.link.external_item( me, ext, item)
            me.q2.tag_tree()
            return r

    class symlink( Operations.symlink, link):
        pass

    class rename( QAspect.rename):
        _on_result_change_name = Archive.rename.item_item.im_func

        def tag_tag( me, fr, to):
            to.id = fr.id
            me.db.save_tag( to)
            del Tags.name2tag[ fr.name]

    class chmod( Archive.chmod):
        def tag( me, t):
            return None
        negative = tag

# vim:ts=4:sw=4:expandtab
