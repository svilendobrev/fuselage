import os
import sys

LOGFILE = ''
DEBUG = 0

def log( *texts):
    if not DEBUG and not LOGFILE: return
    s = ' '.join( [ repr(t) for t in texts ] )
    if DEBUG:
        print >> sys.stderr, '|', s
    if LOGFILE:
        with open( LOGFILE, 'a') as l:
            l.write( s + '\n')

# vim:ts=4:sw=4:expandtab
