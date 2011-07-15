from tags import Tags
from mintags import Mintags
from archive import Archive
from hardlink import Hardlink
from root import Root
from rels import Relations, EqTags, ChildTags, ParentTags
from items import Items, Untagged
from query import Queries
from util.struct import DictAttr

default_enabled_aspects = (
    Root,
    Archive,
    Tags,
    Hardlink,
    Mintags,
    Relations,
    EqTags,
    ChildTags,
    ParentTags,
    Items,
    Untagged,
    Queries,
)

default_repo = '~/.fsrepo'
default_logfile = '~/log.txt' # empty to disable logging
default_storage_type = 'sqlite'

default_query_grammar = DictAttr(
    not_    = '!',
    and_    = '^',
    or_     = '+',
    eval_   = '=',
)

# vim:ts=4:sw=4:expandtab
