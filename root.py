from aspect import Aspect
from operations import Operations
from errno import *
from log import log
import itertools as it
import os
import re

class Root( Aspect):
    _rexp = re.compile('^/$')
    _dir_name = ''

    getattr = Operations.getattr
    access = Operations.access

    class readdir( Operations.readdir):
        def aspect( me):
            return me.direntries( me._get_aspect_dirs())

        def _get_aspect_dirs(me):
            entries = ['.', '..']
            dirs = [ a.dir_name() for a in me.context.aspects ]
            entries += [ d for d in dirs if d ]
            return entries

    class utime( Operations.utime):
        def aspect( me):
            return me.osexec( me.context.archive, me.times)

    class rename( Operations.rename):
        not_implemented = EPERM

    class symlink( Operations.symlink):
        not_implemented = EPERM

    class link( Operations.link):
        not_implemented = EPERM


# vim:ts=4:sw=4:expandtab
