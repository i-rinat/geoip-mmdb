from . import types
from .mmdb import MMDB, MMDBMeta
from .types import SearchTreeNode, SearchTreeLeaf
from .types import Uint16, Uint32, Uint64, Uint128, Int32, Float, Double
import struct


class Reader(object):
    def __init__(self, fname):
        with open(fname, 'rb') as f:
            self.db = f.read()

        self.offset = 0
        self.pointer_cache = {}
        self.leaf_cache = {}
        self.node_cache = {}

        self.metadata_offset = self.db.rfind(types.METADATA_MAGIC)
        if self.metadata_offset < 0:
            raise Exception("no metadata")

        self.metadata_offset += len(types.METADATA_MAGIC)

        meta = self._unserialize(self.metadata_offset)

        self.meta = MMDBMeta()
        self.meta.build_epoch = meta[u'build_epoch'].value
        self.meta.database_type = meta[u'database_type']
        self.meta.description = meta[u'description']
        self.meta.ip_version = meta[u'ip_version'].value
        self.meta.languages = meta[u'languages']
        self.meta.node_count = meta[u'node_count'].value
        self.meta.record_size = meta[u'record_size'].value

        self.data_offset = \
            self.meta.record_size * 2 // 8 * self.meta.node_count + 16

    def get_tree(self):
        return self._read_db()

    def get_meta(self):
        return self.meta

    def _read_leaf(self, idx):
        self.offset = self.data_offset + idx - self.meta.node_count - 16
        offset = self.offset
        if offset in self.leaf_cache:
            return self.leaf_cache[offset]

        leaf = SearchTreeLeaf(self._unserialize())
        self.leaf_cache[offset] = leaf
        return leaf

    def _idx_to_node(self, idx, divein_func):
        if idx < self.meta.node_count:
            if idx in self.node_cache:
                return self.node_cache[idx]

            node = divein_func(idx)
            self.node_cache[idx] = node
            return node

        elif idx == self.meta.node_count:
            return None

        else:
            return self._read_leaf(idx)

    def _read_search_tree_node_24(self, node_idx):
        offset = node_idx * 6
        b1, b2, b3, b4, b5, b6 = struct.unpack_from('>BBBBBB', self.db, offset)
        left_idx = (b1 * 256 + b2) * 256 + b3
        right_idx = (b4 * 256 + b5) * 256 + b6
        divein_func = self._read_search_tree_node_24
        return SearchTreeNode(self._idx_to_node(left_idx, divein_func),
                              self._idx_to_node(right_idx, divein_func))

    def _read_search_tree_node_28(self, node_idx):
        offset = node_idx * 7
        b1, b2, b3, b4, b5, b6, b7 = struct.unpack_from('>BBBBBBB', self.db,
                                                        offset)
        left_idx = (((b4 >> 4) * 256 + b1) * 256 + b2) * 256 + b3
        right_idx = (((b4 & 0x0f) * 256 + b5) * 256 + b6) * 256 + b7

        divein_func = self._read_search_tree_node_28
        return SearchTreeNode(self._idx_to_node(left_idx, divein_func),
                              self._idx_to_node(right_idx, divein_func))

    def _read_search_tree_node_32(self, node_idx):
        offset = node_idx * 8
        left_idx, right_idx = struct.unpack_from('>II', self.db, offset)

        divein_func = self._read_search_tree_node_32
        return SearchTreeNode(self._idx_to_node(left_idx, divein_func),
                              self._idx_to_node(right_idx, divein_func))

    def _read_db(self):
        if self.meta.record_size == 24:
            return self._read_search_tree_node_24(0)

        elif self.meta.record_size == 28:
            return self._read_search_tree_node_28(0)

        elif self.meta.record_size == 32:
            return self._read_search_tree_node_32(0)

        else:
            raise Exception('unknown record size')

    def _unserialize(self, offset=None):
        if offset is not None:
            self.offset = offset

        control_byte, = struct.unpack_from('>B', self.db, self.offset)
        self.offset += 1

        field_type = control_byte >> 5
        field_length = control_byte & 0x1f

        # Non-common field type.
        if field_type == 0:
            field_type = 7 + struct.unpack_from('>B', self.db, self.offset)[0]
            self.offset += 1

        # Large length, variable-length-encoded.
        if field_length == 29:
            b1, = struct.unpack_from('>B', self.db, self.offset)
            self.offset += 1
            field_length = 29 + b1

        elif field_length == 30:
            b1, b2 = struct.unpack_from('>BB', self.db, self.offset)
            self.offset += 2
            field_length = 285 + b1 * 256 + b2

        elif field_length == 31:
            b1, b2, b3 = struct.unpack_from('>BBB', self.db, self.offset)
            self.offset += 3
            field_length = 65821 + b1 * 65536 + b2 * 256 + b3

        # Types.
        if field_type == types.TYPE_POINTER:
            ss_bits = (control_byte >> 3) & 0x03
            vvv_bits = control_byte & 0x07

            if ss_bits == 0:
                b1, = struct.unpack_from('>B', self.db, self.offset)
                self.offset += 1
                pointer = vvv_bits * 256 + b1

            elif ss_bits == 1:
                b1, b2 = struct.unpack_from('>BB', self.db, self.offset)
                self.offset += 2
                pointer = (vvv_bits * 256 + b1) * 256 + b2 + 2048

            elif ss_bits == 2:
                b1, b2, b3 = struct.unpack_from('>BBB', self.db, self.offset)
                self.offset += 3
                pointer = (((vvv_bits * 256 + b1) * 256 + b2) * 256 + b3 +
                           526336)

            else:  # ss_bits == 3
                b1, b2, b3, b4 = struct.unpack_from('>BBBB', self.db,
                                                    self.offset)
                pointer = ((b1 * 256 + b2) * 256 + b3) * 256 + b4

            if pointer in self.pointer_cache:
                return self.pointer_cache[pointer]

            saved_offset = self.offset
            self.offset = self.data_offset + pointer
            value = self._unserialize()
            self.offset = saved_offset
            self.pointer_cache[pointer] = value

            return value

        elif field_type == types.TYPE_UTF8:
            s = self.db[self.offset:self.offset+field_length]
            self.offset += field_length
            return s.decode('utf-8')

        elif field_type == types.TYPE_DOUBLE:
            value, = struct.unpack_from('>d', self.db, self.offset)
            self.offset += 8
            return Double(value)

        elif field_type == types.TYPE_BYTES:
            s = self.db[self.offset:self.offset+field_length]
            self.offset += field_length
            return s

        elif field_type == types.TYPE_UINT16:
            return Uint16(self._read_uint(field_length))

        elif field_type == types.TYPE_UINT32:
            return Uint32(self._read_uint(field_length))

        elif field_type == types.TYPE_MAP:
            m = {}
            for _ in range(field_length):
                key = self._unserialize()
                value = self._unserialize()
                m[key] = value
            return m

        elif field_type == types.TYPE_INT32:
            raise NotImplementedError('int32')

        elif field_type == types.TYPE_UINT64:
            return Uint64(self._read_uint(field_length))

        elif field_type == types.TYPE_UINT128:
            return Uint128(self._read_uint(field_length))

        elif field_type == types.TYPE_ARRAY:
            a = []
            for _ in range(field_length):
                a.append(self._unserialize())
            return a

        elif field_type == types.TYPE_DATA_CACHE_CONTAINER:
            raise NotImplementedError("data cache container")

        elif field_type == types.TYPE_END_MARKER:
            raise NotImplementedError("end marker")

        elif field_type == types.TYPE_BOOLEAN:
            return field_length > 0

        elif field_type == types.TYPE_FLOAT:
            value, = struct.unpack_from('>f', self.db, self.offset)
            self.offset += 4
            return Float(value)

        else:
            raise NotImplementedError('unknown type {}'.format(field_type))

    def _read_uint(self, length):
        n = 0
        for _ in range(length):
            b, = struct.unpack_from('>B', self.db, self.offset)
            self.offset += 1
            n = n * 256 + b

        return n


def read_database(fname):
    reader = Reader(fname)
    return MMDB(reader.get_tree(), reader.get_meta())
