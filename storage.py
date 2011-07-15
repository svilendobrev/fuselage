from log import log
import sqlite3
import itertools as it

class Storage:
    event_listeners = {}

    @staticmethod
    def event_notifier( event, before =None, after =None):
        def notifier( *a, **ka):
            if before: before( *a, **ka)
            res = event( *a, **ka)
            if after: after( *a, **ka)
            return res
        return notifier

    @classmethod
    def _include( me, storage):
        for name, val in vars(storage).iteritems():   # no inherited methods here
            if callable( val):
                setattr( me, name, val)

    @classmethod
    def setup_listeners( me):
        for event, listeners in me.event_listeners.iteritems():
            for l in listeners:
                exec_and_notify = me.event_notifier( getattr( me, event), **l)
                setattr( me, event, exec_and_notify)


def register( before_after, event):
    def registered( meth):
        Storage.event_listeners.setdefault( event, []).append( { before_after : meth })
        return meth
    return registered

def exec_before( event):
    return register('before', event)
def exec_after( event):
    return register('after', event)

from sql import Schema, Constraint
import os
class sqlite( Storage):
    conn = None
    schema = Schema()

    def __init__( me, context, memory =False):
        name = me.name = memory and ':memory:' or os.path.join( context.repository, 'db.sql')
        if not me.conn:
            log('open db name: ', name)
            me.__class__.conn = sqlite3.connect( name)
        me.create_schema()

    def create_schema( me):
        for name, columns in me.schema.iter_over('tables'):
            me._create_table( name, **columns)
        for name, index in me.schema.iter_over('indexes'):
            me._create_index( name, index.table, *index.columns)
        for name, trigger in me.schema.iter_over('triggers'):
            me._create_trigger( name, trigger.when, *trigger.what)

    def drop_schema( me):
        s = 'drop %(typ)s if exists %(name)s'
        for typ in me.schema.target_types:
            for name, ignored in me.schema.iter_over( typ):
                me.conn.execute( s % locals())

    @classmethod
    def include( me, db):
        sch = getattr( db, 'schema', None)
        if sch:
            me.schema.update( sch)
        me._include( db)

    def _create_table( me, table_, **columns):
        constraints = dict( (k,v) for k,v in columns.iteritems() if isinstance(v, Constraint))
        for k in constraints:
            del columns[k]
        s = 'create table if not exists %(table_)s' % locals()
        s += '('
        s += ', '.join([ '%s %s' % (k,v) for k,v in columns.iteritems() ])
        if constraints:
            s += ', '+', '.join([ 'constraint %s %s' % (k,v) for k,v in constraints.iteritems() ])
        s += ')'
        me.conn.execute( s)

    def _create_index( me, index_, table_, *columns):
        s = 'create index if not exists %(index_)s on %(table_)s' % locals()
        s += '(' + ','.join( columns)+')'
        me.conn.execute( s)

    def _create_trigger( me, trigger_, when_, *what_):
        s = 'create trigger if not exists %(trigger_)s %(when_)s begin\n' % locals()
        s += '\n'.join( [w+';' for w in what_])
        s += 'end'
        me.conn.execute( s)

    def sql( me, s, *values):
        log('sqlllll:', s)
        me.conn.execute( s, values)

    def sql_many( me, s, values):
        log('sqlllll many:', s)
        me.conn.executemany( s, values)

    def query( me, sql, *values):
        log('queryyy:', sql)
        c = me.conn.cursor()
        c.execute( sql, values)
        res = c.fetchall()
        c.close()
        return res

    def first( me, r):
        return None if not r else r[0]

    def value( me, rows):
        return me.first( me.first( rows))

    def commit( me):
        me.conn.commit()

    def rollback( me):
        me.conn.rollback()

    def last_insert_rowid( me):
        return me.value( me.query('select last_insert_rowid()'))


if __name__ == '__main__':
    import sys
    fname = sys.argv[1:] and sys.argv[1]
    if fname:
        db = DB( fname)
        print 'all_tags', db.all_tags()
    else:
        db = DB('/tmp/zzz.sql')
        db.create()
        db.create_tag('zzz')

        print db.get_tag_id('zzz')
        db.commit()

# vim:ts=4:sw=4:expandtab
