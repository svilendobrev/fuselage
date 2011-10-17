from tags import Tags
from mintags import Mintags
from archive import Archive
from hardlink import Hardlink
from root import Root
import rels
from items import Items, Untagged
from metatags import MetaTags
from queries import Queries
from util.struct import DictAttr

# order is optionally used for precedence for 2-address operations;
# the more sophisticated the smaller the index in the list of aspects
default_enabled_aspects = (
    rels.In,
    rels.Has,
    rels.Is,

    MetaTags,
    Items,
    Mintags,
    Tags,
    Untagged,
    Archive,
    Queries,
    Root,
    Hardlink,
)

default_repo = '~/.fsrepo'
default_logfile = '~/log.txt' # empty to disable logging
default_storage_type = 'sqlite'

default_query_grammar = DictAttr(
    not_    = '!',
    and_    = '^',
    or_     = '+',
    eval_   = '=',
    #root    = '#',
)

# vim:ts=4:sw=4:expandtab
