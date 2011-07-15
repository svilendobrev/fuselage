class Schema:
    storage_name = 'sqlite'
    target_types = 'tables indexes triggers'.split()
    def __init__( me, *refs, **data):
        for n in me.target_types:
            setattr( me, n, data.get( n, {}))
        me.refs = {}
        for r in refs:
            for n in r.names:
                me.refs.setdefault( r.target_type,{})[ n] = r.aspect

    def update( me, o):
        for name, table in o.iter_over('tables'):
            if name in me.tables:
                me.tables[ name].update( table)
            else:
                me.tables[ name] = table
        me.indexes.update( o.iter_over('indexes'))
        me.triggers.update( o.iter_over('triggers'))

    def iter_over( me, typ):
        for name, aspect in me.refs.get( typ, {}).iteritems():
            storage = getattr( aspect, me.storage_name)
            yield name, getattr( storage.schema, typ)[ name]
        for name, val in getattr(me, typ).iteritems():
            yield name, val

class Ref:
    target_type = None
    storage_name = 'sqlite'
    def __init__( me, aspect, *names):
        me.aspect = aspect
        me.names = names

    def get_targets( me):
        storage = getattr( me.aspect, me.storage_name)
        t = getattr( storage.schema, me.target_type)
        return dict( (n,t[n]) for n in me.names)

class RefTables( Ref):
    target_type = 'tables'
class RefIndexes( Ref):
    target_type = 'indexes'
class RefTriggers( Ref):
    target_type = 'triggers'

Table = dict
class Constraint(str): pass

class Index:
    def __init__( me, table, *columns):
        me.table = table
        me.columns = columns

class Trigger:
    def __init__( me, when, *what):
        me.when = when
        me.what = what


# vim:ts=4:sw=4:expandtab
