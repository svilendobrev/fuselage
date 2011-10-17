from sql import Table, Schema, Index
import model
from operations import Operations
from aspect import Aspect
from log import log
from errno import *
import os
import os.path

class Queries( Aspect):
    class Shortcut( model.NameId):
        path = None

    class sqlite:
        schema = Schema(
            tables = dict(
                queries = Table(
                    id      ='integer not null primary key autoincrement',
                    name    ='varchar(65) unique not null',
                    path    ='text(255) not null',
                ),
            ),
            indexes = dict( shortcut_name_index = Index('queries', 'name')),
        )
        def save_shortcut( me, q):
            log('save_shortcut:', q)
            if not q.id:
                me.sql('insert into queries (name, path) values (?,?)', q.name, q.path)
                q.id = me.last_insert_rowid()
                return q.id
            me.sql('update queries set name=? path=? where id=?', q.name, q.path, q.id)

        def delete_shortcut( me, q):
            me.sql('delete from queries where id = ?', q.id)

        def get_shortcut( me, name):
            d = me.first( me.query('select id, name, path from queries where name=?', name))
            if d:
                id_, name, path = d
                return Queries.Shortcut( id=id_, name=name, path=path)
            return None

        def get_all_shortcuts( me):
            return [ Queries.Shortcut( id=id_, name=name, path=path)
                    for id_, name, path in me.query('select id, name, path from queries') ]

    class Parser( Aspect.Parser):
        def init( me):
            me.shortcut = Queries.Shortcut()

        def _parse( me):
            if os.path.sep in me.path_in_aspect:
                return 'error'
            me.shortcut = me.context.db.get_shortcut( me.path_in_aspect) or Queries.Shortcut( name=me.path_in_aspect)
            return me.shortcut

    class getattr( Operations.getattr):
        def shortcut( me, o):
            if o.id:
                return me.as_symlink( me.aspect())
            return ENOENT

    class readdir( Operations.readdir):
        def aspect( me):
            return me.direntries([ q.name for q in me.db.get_all_shortcuts() ])

    class unlink( Operations.unlink):
        def shortcut( me, o):
            me.db.delete_shortcut( o)

    class link( Operations.link):
        def shortcut_shortcut( me, fr, to):
            if fr.name == to.name: return
            to.path = fr.path
            me.db.save_shortcut( to)

    class symlink( Operations.symlink):
        def external_shortcut( me, ext, shortcut):
            shortcut.path = ext.path
            me.db.save_shortcut( shortcut)

    class rename( Operations.rename):
        def shortcut_shortcut( me, old, new):
            old.name = new.name
            me.db.save_shortcut( old)

        def query_shortcut( me, shcut):
            shcut.path = me.q1.full_path
            me.db.save_shortcut( shcut)

    class readlink( Operations.readlink):
        def shortcut( me, o):
            path = os.path.join( me.context.mountpoint, Queries.dir_name(), o.path)
            return os.path.abspath( path)

# vim:ts=4:sw=4:expandtab
