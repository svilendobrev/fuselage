from aspect import QAspect
from storage import exec_after
from tags import Tags
from sql import Table, Index, UsesTables, Schema, Constraint
from log import log
from errno import *
import sqlite3

class MetaTags( QAspect):
    class MetaTag( Tags.Tag): pass

    operandType = MetaTag
    resultType = Tags.Tag

    class sqlite:
        schema = Schema(
            UsesTables( Tags, 'tags'),
            tables = dict(
                metatags = Table(
                    id      ='integer not null primary key autoincrement',
                    name    ='varchar(65) unique not null',
                ),
                metatagging = Table(
                    tag_id  ='integer not null',
                    meta_id ='integer not null',
                    joint_id ='integer not null',
                    metatagging_key=Constraint('unique (tag_id, meta_id, joint_id)'),
                ),
            ),
            indexes = dict(
                metatagging_index = Index('metatagging', 'tag_id', 'meta_id', 'joint_id'),
            ),
        )

        # metatags
        def save_meta( me, metatag):
            if not metatag.id:
                log('create_metatag:', metatag)
                me.sql('insert into metatags(name) values(?)', metatag.name)
                metatag.id = me.last_insert_rowid()
            else:
                me.sql('update metatags set name=? where id=?', metatag.name, metatag.id)
            MetaTags.name2metatag[ metatag.name] = metatag

        def delete_meta( me, metatag):
            me.sql('delete from metatags where id = ?', metatag.id)

            joints = [ Tags.Tag(id=id_, name=name)
                        for id_, name in me.query('''
select tags.id as tag_id, tags.name as tag_name
    from tags
    join metatagging on tags.id == metatagging.joint_id
    where metatagging.meta_id=?''', metatag.id) ]
            for j in joints:
                me.delete_tag( j)
            del MetaTags.name2metatag[ metatag.name ]

        def get_all_metatags( me):
            q = me.query('select id, name from metatags')
            return [ MetaTags.MetaTag( id=id_, name=name) for id_, name in q ]

        # tag and metatags
        def get_tag_metatags( me, tag):
            q = me.query('''
select metatags.id as meta_id, metatags.name as meta_name from metatags
    join metatagging on metatags.id == metatagging.meta_id
    where metatagging.tag_id = ?''', tag.id)
            return [ MetaTags.MetaTag( id=id_, name=name) for id_, name in q ]

        def join_tag_meta( me, tag, *metatags):
            for meta in metatags:
                joint = Tags.Tag( name=MetaTags.joint_name( meta, tag))
                me.save_tag( joint)
                me.sql('insert into metatagging(meta_id, tag_id, joint_id) values(?,?,?)', meta.id, tag.id, joint.id)

        @exec_after('delete_tag')
        def unjoin_tag_meta( me, tag, *metatags):
            if not metatags:
                me.sql('delete from metatagging where tag_id = ? or joint_id = ?', tag.id, tag.id)
            elif len(metatags) == 1:
                meta_id = metatags[0].id
                me.sql('delete from metatagging where meta_id = ? and tag_id = ?', meta_id, tag.id)
            else:
                values = [ m.id for m in metatags ] + [ tag.id ]
                me.sql('delete from metatagging where meta_id in (%s) and tag_id = ?' % ','.join('?'*len(metatags)), *values)

        build_metatags_sql = QAspect.sqlite.make_sql_template(
            sql_intersect = '''
select tags.id as tag_id, tags.name as tag_name from tags
    join metatagging on metatagging.tag_id = tags.id
    join metatags on metatags.id = metatagging.meta_id
    where %s'''
            , sql_select = 'select distinct tag_id, tag_name from ('
            , where4operand  = 'metatags.id = %d'
            , where4negative = 'tags.id not in (select tag_id from metatagging where meta_id = %d)'
            , where4operand_many = 'metatags.id in %s'
            , where4negative_many = 'tags.id not in (select tag_id from metatagging where meta_id in %s)'
        )
        def query_by_metatags( me, tree):
            return [ Tags.Tag( id=id_, name=name) for id_,name in me.query( me.build_metatags_sql( tree)) ]


    class Parser( QAspect.Parser):
        def init( me):
            me.tag = Tags.Tag()
            me.metatag = MetaTags.MetaTag()

        def setup_result( me, path):
            me.tag = me.db.get_tag_by_name( path) or Tags.Tag( name=path)
            if me.tag.id:
                tags = set( me.db.query_by_metatags( me.tree))
                if me.tag not in tags:
                    me.tag.id = None

        def token2operand( me, token):
            return MetaTags.name2metatag.get( token) or MetaTags.MetaTag( name=token)

        def _get_operands4result( me, tag):
            return me.db.get_tag_metatags( tag)
        def _tag_result( me, tag, metatags):
            me.db.join_tag_meta( tag, *metatags)
            Tags.init( me.context)
        def _untag_result( me, tag, metatags):
            me.db.unjoin_tag_meta( tag, *metatags)
            Tags.init( me.context)

    ##################

    name2metatag = dict()

    @classmethod
    def joint_name( me, meta, tag):
        return meta.name + '_' + tag.name

    @classmethod
    def init( me, context):
        name2metatag = MetaTags.name2metatag
        for m in context.db.get_all_metatags():
            name2metatag[ m.name] = m

    class getattr( QAspect.getattr):
        def tag( me, t):
            return me.as_file( me.query())

    class readdir( QAspect.readdir):
        def _get_results( me):
            d = {}
            for o in me.db.query_by_metatags( me.q.tree):
                d[ o.name] = o
            return d

        def _iter_operands( me):
            for m in MetaTags.name2metatag.itervalues():
                yield m.name, m

    class rmdir( QAspect.rmdir):
        def metatag( me, m):
            me.db.delete_meta( m)

    class mkdir( QAspect.mkdir):
        def metatag( me, m):
            try:
                me.db.save_meta( m)
                MetaTags.name2metatag[ m.name] = m
            except sqlite3.DatabaseError, e:
                log('error:', e)
                return EFAULT

    class rename( QAspect.rename):
        _on_result_change_name = Tags.rename.tag_tag.im_func

        def metatag_metatag( me, fr, to):
            to.id = fr.id
            me.db.save_meta( to)
            del MetaTags.name2metatag[ fr.name]

    class chmod( Tags.chmod):
        def metatag( me, m):
            return None
        negative = metatag


# vim:ts=4:sw=4:expandtab
