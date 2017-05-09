from . import types
from .types import Uint16, Uint32, Uint64, Uint128, Int32, Float, Double
from .types import SearchTreeLeaf, SearchTreeNode
from .writer import Writer
from copy import deepcopy
import time


class MMDBMeta(object):
    def __init__(self):
        self.build_epoch = 0
        self.database_type = u'Unknown'
        self.description = {u'en': u''}
        self.ip_version = 6
        self.languages = []
        self.node_count = 0
        self.record_size = 0

    def get(self):
        return {u'binary_format_major_version': Uint16(2),
                u'binary_format_minor_version': Uint16(0),
                u'build_epoch': Uint64(self.build_epoch),
                u'database_type': self.database_type,
                u'description': self.description,
                u'ip_version': Uint16(self.ip_version),
                u'languages': self.languages,
                u'node_count': Uint32(self.node_count),
                u'record_size': Uint16(self.record_size)}

    def clone(self):
        m = MMDBMeta()
        m.build_epoch = int(self.build_epoch)
        m.database_type = deepcopy(self.database_type)
        m.description = deepcopy(self.description)
        m.ip_version = int(self.ip_version)
        m.languages = deepcopy(self.languages)
        m.node_count = int(self.node_count)
        m.record_size = int(self.record_size)
        return m


class MMDB(object):
    def __init__(self, tree, meta):
        self.tree = tree
        self.meta = meta.clone()

    def write(self, fname):
        writer = Writer(self.tree, self.meta)
        writer.write(fname)


def walk_tree(mmdb, visitor_leaf=None, visitor_node=None):
    def walk_tree_impl(node, path, visitor_leaf, visitor_node):
        if type(node) is SearchTreeNode:
            if visitor_node is not None:
                visitor_node(node, path)

            walk_tree_impl(node.left, path + (0,), visitor_leaf, visitor_node)
            walk_tree_impl(node.right, path + (1,), visitor_leaf, visitor_node)

        elif type(node) is SearchTreeLeaf:
            if visitor_leaf is not None:
                visitor_leaf(node, path)

        elif node is None:
            # No information about particular network.
            pass

        else:
            print node
            raise Exception('Unknown node type')

    walk_tree_impl(mmdb.tree, (), visitor_leaf, visitor_node)


def path_to_ip(path):
    if len(path) >= 128 - 32:
        # ipv4
        path = path[128-32:]
        mask_len = len(path)
        path = path + (0,) * (32 - mask_len)
        octets = (int(''.join(str(c) for c in path[k*8:(k+1)*8]), 2)
                  for k in range(4))
        return '.'.join(str(k) for k in octets) + '/' + str(mask_len)

    else:
        # ipv6
        mask_len = len(path)
        path = path + (0,) * (128 - mask_len)
        parts = (int(''.join(str(c) for c in path[k*16:(k+1)*16]), 2)
                 for k in range(8))
        return (':'.join('{:04x}'.format(k) for k in parts) + '/' +
                str(mask_len))


def dump_tree(mmdb):
    def visitor_leaf(node, path):
        print(path_to_ip(path))
        print(node.value)

    walk_tree(mmdb, visitor_leaf)
