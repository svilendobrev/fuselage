from aspect import Aspect, id_and_name
from log import log
from operations import Operations
from sql import Table, Schema, Trigger, Index
import model
from util.attr import isiterable
from util.struct import DictAttr
from errno import *
from stat import *
import os

class Archive( Aspect):
    class Item( model.NameId):
        full_path = None # ~/repo/archive/NNN___alabala

        def set_full_path( me, context):
            s = id_and_name.make( me.id or 0, me.name)
            me.full_path = os.path.join( context.archive, s)

    class sqlite:
        schema = Schema(
            tables = dict(
                items = Table(
                    id      ='integer not null primary key autoincrement',
                    name    ='text(255) not null',
                ),
            ),
            indexes = dict( item_name_index = Index('items', 'name')),
        )
        def get_item( me, id_=None, name=None):
            where = []
            args = []
            if id_:
                where.append('id=?')
                args.append( id_)
            if name:
                where.append('name=?')
                args.append( name)
            s = 'select id, name from items' + (' where ' if where else '')+' and '.join(where)
            return [ Archive.Item( id=_id, name=name) for _id, name in me.query( s, *args) ]

        def save_item( me, item):
            if not item.id:
                log('create_item:', item)
                me.sql('insert into items (name) values (?)', item.name)
                item.id = me.last_insert_rowid()
                return item.id
            me.sql('update items set name=? where id=?', item.name, item.id)

        def delete_item( me, item):
            me.sql('delete from items where id = ?', item.id)

    class Parser( Aspect.Parser):
        def init( me):
            me.item = Archive.Item()

        def _parse( me):
            me.setup_from_path( me.path_in_aspect)
            if me.item.id:
                found = me.context.db.get_item( me.item.id, me.item.name)
                if len(found) != 1:
                    me.item.name = os.path.split( me.path_in_aspect)[1]
                    me.item.id = None
            me.item.set_full_path( me.context)
            return me.item

        def setup_from_path( me, path):
            me.item.name = id_and_name.strip_id_from_path( path)
            me.item.id = id_and_name.get_id_from_path( path)
            #me.item.set_full_path( me.context)

        def setup_from_alias( me, obj):
            me.item = obj
            return obj

        def __str__( me):
            return str(me.__class__) + ' '+ str(me.item)
        __repr__ = __str__

    ##########

    class getattr( Operations.getattr):
        def item( me, o):
            return me.osexec( o.full_path) if o.id else ENOENT

    class access( Operations.access):
        def aspect( me):
            return None if os.access( me.context.archive, getattr(me, 'mode', 0)) else EACCES

    class open( Operations.open):
        def item( me, o):
            from mount import FsFile
            return FsFile( o.full_path, me.flags)

    class readdir( Operations.readdir):
        def _get_items( me):
            return me.db.get_item()
        def aspect( me):
            names = []
            for item in me._get_items():
                s = id_and_name.make( item.id, item.name)
                names.append( s)
                item.set_full_path( me.context)
                Archive.aliases[ os.path.join( me.q.full_path, s)] = item
            return me.direntries( names)

    class mknod( Operations.mknod):
        def item( me, o):
            me.db.save_item( o)
            o.set_full_path( me.context)
            Archive.aliases[ me.q.full_path] = o
            return me.osexec( o.full_path, *me.args)

    class unlink( Operations.unlink):
        def item( me, o):
            me.db.delete_item( o)
            return me.osexec( o.full_path)

    '''
    # directories not supported
    class mkdir( Operations.mkdir, mknod): pass
    class rmdir( Operations.rmdir, unlink): pass
    '''

    class link( Operations.link):
        def _link( me, from_path, item):
            me.db.save_item( item)
            item.set_full_path( me.context)
            Archive.aliases[ me.q2.full_path] = item
            return me.osexec( from_path, item.full_path)

        def item_item( me, fr, to):
            return me._link( fr.full_path, to)
        def external_item( me, ext, item):
            item.name = item.name or os.path.split( ext.path)[1]
            return me._link( ext.path, item)

    class symlink( Operations.symlink, link):
        pass

    class rename( Operations.rename):
        def item_item( me, old, new):
            qfrom, qto = me.q1, me.q2
            new.id = old.id
            new.name = id_and_name.strip_id_from_path( new.name) #XXX is this necessary?
            me.db.save_item( new)
            new.set_full_path( me.context)
            res = me.osexec( old.full_path, new.full_path)
            Archive.aliases[ me.q2.full_path] = new
            return res

    class _directly_call_os( object):
        def item( me, o):
            return me.osexec( o.full_path, *me.args)

    class readlink( Operations.readlink, _directly_call_os): pass
    class chmod( Operations.chmod, _directly_call_os): pass
    class chown( Operations.chmod, _directly_call_os): pass
    class utime( Operations.utime, _directly_call_os): pass


# vim:ts=4:sw=4:expandtab
