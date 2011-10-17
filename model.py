
class Model:
    id = None
    def __init__( me, **kargs):
        for k,v in kargs.iteritems():
            setattr( me, k, v)
    def __str__( me):
        return me.__class__.__name__ + ' '+ '\n'.join([ '%s=%s' % (k,repr(getattr(me, k))) for k in vars(me) if not callable( getattr( me, k))])
    __repr__ = __str__

    def __hash__( me):
        return hash( me.id)
    def __cmp__( me, o):
        return cmp( me.id, o.id)
    def __eq__( me, o):
        if o is None:
            return False
        return not bool(me.__cmp__( o))


class NameId( Model):
    name = None

# vim:ts=4:sw=4:expandtab
