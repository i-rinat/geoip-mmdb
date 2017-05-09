from . import types
from .types import SearchTreeNode, SearchTreeLeaf
from .types import Uint16, Uint32, Uint64, Uint128, Int32, Float, Double
import math
import struct


class Writer(object):
    def __init__(self, tree, meta):
        self.tree = tree
        self.meta = meta

    def _adjust_record_size(self):
        # Tree records should be large enough to contain either tree node index
        # or data offset.
        max_id = self.meta.node_count + self._data_pointer + 1

        # Estimate required bit count.
        bit_count = int(math.ceil(math.log(max_id, 2)))
        if bit_count <= 24:
            self.meta.record_size = 24
        elif bit_count <= 28:
            self.meta.record_size = 28
        elif bit_count <= 32:
            self.meta.record_size = 32
        else:
            raise Exception('record_size > 32')

        self.data_offset = \
            self.meta.record_size * 2 / 8 * self.meta.node_count

    def _enumerate_nodes(self, node):
        if type(node) is SearchTreeNode:
            node_id = id(node)
            if node_id not in self._node_idx:
                self._node_idx[node_id] = self._node_counter
                self._node_counter += 1
                self._node_list.append(node)

            self._enumerate_nodes(node.left)
            self._enumerate_nodes(node.right)

        elif type(node) is SearchTreeLeaf:
            if 'serialized' not in node.__dict__:
                node.serialized = self._serialize_value(node.value, True)
                node.data_offset = self._data_pointer
                self._data_list.append(node.serialized)
                self._data_pointer += len(node.serialized)

        else:  # == None
            return

    def write(self, fname):
        self._node_counter = 0
        self._node_list = []
        self._data_pointer = 16
        self._data_list = []
        self._leaf_offset = {}
        self._data_cache = {}
        self._node_idx = {}
        self._enumerate_nodes(self.tree)
        self._adjust_record_size()

        def calc_record_idx(node):
            if node is None:
                return self.meta.node_count
            elif type(node) is SearchTreeNode:
                return self._node_idx[id(node)]
            elif type(node) is SearchTreeLeaf:
                return node.data_offset + self.meta.node_count
            else:
                raise Exception("unexpected type")

        with open(fname, 'wb') as f:
            for node in self._node_list:
                left_idx = calc_record_idx(node.left)
                right_idx = calc_record_idx(node.right)

                if self.meta.record_size == 24:
                    b1 = (left_idx >> 16) & 0xff
                    b2 = (left_idx >> 8) & 0xff
                    b3 = left_idx & 0xff
                    b4 = (right_idx >> 16) & 0xff
                    b5 = (right_idx >> 8) & 0xff
                    b6 = right_idx & 0xff
                    f.write(struct.pack('>BBBBBB', b1, b2, b3, b4, b5, b6))

                elif self.meta.record_size == 28:
                    b1 = (left_idx >> 16) & 0xff
                    b2 = (left_idx >> 8) & 0xff
                    b3 = left_idx & 0xff
                    b4 = ((left_idx >> 24) & 0xf) * 16 + \
                         ((right_idx >> 24) & 0xf)
                    b5 = (right_idx >> 16) & 0xff
                    b6 = (right_idx >> 8) & 0xff
                    b7 = right_idx & 0xff
                    f.write(struct.pack('>BBBBBBB', b1, b2, b3, b4, b5, b6,
                                        b7))

                elif self.meta.record_size == 32:
                    f.write(struct.pack('>II', left_idx, right_idx))

                else:
                    raise Exception('self.meta.record_size > 32')

            f.write(b'\x00' * 16)

            for element in self._data_list:
                f.write(element)

            f.write(types.METADATA_MAGIC)
            f.write(self._serialize_value(self.meta.get()))

    def _make_value_header(self, type_, length):
        if length >= 16843036:
            raise Exception('length >= 16843036')

        elif length >= 65821:
            five_bits = 31
            length -= 65821
            b3 = length & 0xff
            b2 = (length >> 8) & 0xff
            b1 = (length >> 16) & 0xff
            additional_length_bytes = struct.pack('>BBB', b1, b2, b3)

        elif length >= 285:
            five_bits = 30
            length -= 285
            b2 = length & 0xff
            b1 = (length >> 8) & 0xff
            additional_length_bytes = struct.pack('>BB', b1, b2)

        elif length >= 29:
            five_bits = 29
            length -= 29
            additional_length_bytes = struct.pack('>B', length & 0xff)

        else:
            five_bits = length
            additional_length_bytes = b''

        if type_ <= 7:
            res = struct.pack('>B', (type_ << 5) + five_bits)
        else:
            res = struct.pack('>BB', five_bits, type_ - 7)

        return res + additional_length_bytes

    def _make_pointer(self, pointer):
        if pointer >= 134744064:
            res = struct.pack('>BI', 0x38, pointer)

        elif pointer >= 526336:
            pointer -= 526336
            res = struct.pack('>BBBB', 0x30 + ((pointer >> 24) & 0x07),
                              (pointer >> 16) & 0xff, (pointer >> 8) & 0xff,
                              pointer & 0xff)

        elif pointer >= 2048:
            pointer -= 2048
            res = struct.pack('>BBB', 0x28 + ((pointer >> 16) & 0x07),
                              (pointer >> 8) & 0xff, pointer & 0xff)

        else:
            res = struct.pack('>BB', 0x20 + ((pointer >> 8) & 0x07),
                              pointer & 0xff)

        return res

    def _serialize_unsigned(self, value, type_, maxlen):
        res = b''
        while value != 0 and len(res) < maxlen:
            res = struct.pack('>B', value & 0xff) + res
            value = value >> 8
        return res

    def _serialize_value(self, value, use_cache=False):
        if use_cache:
            if id(value) in self._data_cache:
                return self._make_pointer(self._data_cache[id(value)])

        if type(value) is dict:
            res = self._make_value_header(types.TYPE_MAP, len(value))
            for k, v in value.items():
                # Keys are always stored by value.
                res += self._serialize_value(k, use_cache)
                res += self._serialize_value(v, use_cache)

        elif isinstance(value, type(u'')):
            encoded_value = value.encode('utf-8')
            res = self._make_value_header(2, len(encoded_value))
            res += encoded_value

        elif type(value) is Uint32:
            res = self._serialize_unsigned(value.value, types.TYPE_UINT32, 4)
            res = self._make_value_header(types.TYPE_UINT32, len(res)) + res

        elif type(value) is Uint16:
            res = self._serialize_unsigned(value.value, types.TYPE_UINT16, 2)
            res = self._make_value_header(types.TYPE_UINT16, len(res)) + res

        elif type(value) is Uint64:
            res = self._serialize_unsigned(value.value, types.TYPE_UINT64, 8)
            res = self._make_value_header(types.TYPE_UINT64, len(res)) + res

        elif type(value) is list:
            res = self._make_value_header(types.TYPE_ARRAY, len(value))
            for k in value:
                res += self._serialize_value(k, use_cache)

        elif type(value) is Double:
            res = self._make_value_header(types.TYPE_DOUBLE, 8)
            res += struct.pack('>d', value.value)

        elif type(value) is bool:
            res = self._make_value_header(types.TYPE_BOOLEAN,
                                          1 if value else 0)

        else:
            raise Exception("don't know how to serialize {}".
                            format(type(value)))

        if use_cache:
            self._data_cache[id(value)] = self._data_pointer - 16
            self._data_list.append(res)
            self._data_pointer += len(res)
            return self._serialize_value(value, True)

        return res
