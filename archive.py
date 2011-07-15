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
        full_path = None    # ~/repo/archive/NNN___alabala

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
            return [ Archive.Item( id=id_, name=name) for id_, name in me.query( s, *args) ]

        def save_item( me, item):
            if not item.id:
                log('create_item:', item)
                me.sql('insert into items (name) values (?)', item.name)
                item.id = me.value(me.query('select last_insert_rowid()'))
                return item.id
            me.sql('update items set name=? where id=?', item.name, item.id)

        def delete_item( me, item):
            me.sql('delete from items where id = ?', item.id)

    class Parser( Aspect.Parser):
        def init( me):
            me.item = Archive.Item()

        def _parse( me):
            me.setup_from_path( me.path_in_aspect)
            return me.item

        def setup_from_path( me, path):
            me.set_item_name( id_and_name.strip_id_from_path( path))

        def setup_from_alias( me, obj):
            me.item = obj
            me.set_item_full_path()
            return obj

        def set_item_name( me, name):
            me.item.name = name
            if not me.item.id:
                me.item.id = id_and_name.get_id_from_path( me.full_path)
            me.set_item_full_path()

        def set_item_full_path( me):
            s = id_and_name.make( me.item.id, me.item.name) if me.item.name else ''
            me.item.full_path = os.path.join( me.context.archive, s)

        def __str__( me):
            return str(me.__class__) + ' '+ str(me.item)
        __repr__ = __str__

    ##########

    class getattr( Operations.getattr):
        def item( me, o):
            return me.osexec( o.full_path)

    class access( Operations.access):
        def item( me, o):
            return None if os.access( o.full_path, getattr(me, 'mode', 0)) else EACCES

    class open( Operations.open):
        def item( me, o):
            from mount import FsFile
            return FsFile( o.full_path, me.flags)

    class readdir( Operations.readdir):
        def _item_names( me, excl =()):
            items = {}
            for o in me.db.get_item():
                if o in excl: continue
                if o.name in items:
                    olditem = items.pop( o.name)
                    items[ id_and_name.make( olditem.id, olditem.name)] = olditem
                    items[ id_and_name.make( o.id, o.name)] = o
                else:
                    items[ o.name] = o
            names = items.keys()
            for name,item in items.iteritems():
                Archive.aliases[ os.path.join( me.q.full_path, name)] = item
            return names

        def aspect( me):
            return me.direntries( me._item_names())

        def item( me, o):
            res = me.osexec( o.full_path)
            if isiterable( res):
                return me.direntries( res)
            return res

    class _create_item_mixin( object):
        def create_item( me, q):
            id_ = me.db.save_item( q.item)
            q.set_item_full_path()
            Archive.aliases[ q.full_path] = q.item
            return id_

    class mknod( Operations.mknod, _create_item_mixin):
        def item( me, o):
            me.create_item( me.q)
            return me._do_osfunc()

        def _do_osfunc( me):
            return me.osexec( me.q.item_full_path, me.mode, me.dev)

    class mkdir( Operations.mkdir, mknod):
        def _do_osfunc( me):
            me.osexec( me.q.item.full_path, getattr(me, 'mode', 755))
            #os.chmod( me.q.item_full_path, S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH)

    class unlink( Operations.unlink):
        def item( me, o):
            me.db.delete_item( o)
            return me.osexec( o.full_path)

    class rmdir( Operations.rmdir, unlink):
        pass

    class link( Operations.link, _create_item_mixin):
        #def item_item( me):
        def external_item( me, ext, item):
            path_from = me.q1.full_path
            qto = me.q2
            filename = item.name or os.path.split( path_from)[1]
            qto.set_item_name( filename)
            log('external_itemm: ', filename)
            id_ = me.create_item( qto)
            return me.osexec( path_from, item.full_path)

    class symlink( Operations.symlink, link):
        pass

    class rename( Operations.rename):
        def item_item( me, old, new):
            qfrom, qto = me.q1, me.q2
            new.id = old.id
            qto.setup_from_path( id_and_name.strip_id_from_path( new.name))
            me.db.save_item( new)
            return me.osexec( old.full_path, new.full_path)

    class _directly_call_os( object):
        def item( me, o):
            return me.osexec( o.full_path, *me.args)

    class readlink( Operations.readlink, _directly_call_os): pass
    class chmod( Operations.chmod, _directly_call_os): pass
    class chown( Operations.chmod, _directly_call_os): pass
    class utime( Operations.utime, _directly_call_os): pass


# vim:ts=4:sw=4:expandtab
