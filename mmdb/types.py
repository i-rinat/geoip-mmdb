METADATA_MAGIC = b'\xab\xcd\xefMaxMind.com'
TYPE_POINTER = 1
TYPE_UTF8 = 2
TYPE_DOUBLE = 3
TYPE_BYTES = 4
TYPE_UINT16 = 5
TYPE_UINT32 = 6
TYPE_MAP = 7
TYPE_INT32 = 8
TYPE_UINT64 = 9
TYPE_UINT128 = 10
TYPE_ARRAY = 11
TYPE_DATA_CACHE_CONTAINER = 12
TYPE_END_MARKER = 13
TYPE_BOOLEAN = 14
TYPE_FLOAT = 15


class MMDBNumber(object):
    class_name = 'MMDBNumber'

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "{}({})".format(self.class_name, self.value)


class Uint16(MMDBNumber):
    class_name = 'Uint16'


class Uint32(MMDBNumber):
    class_name = 'Uint32'


class Uint64(MMDBNumber):
    class_name = 'Uint64'


class Uint128(MMDBNumber):
    class_name = 'Uint128'


class Int32(MMDBNumber):
    class_name = 'Int32'


class Float(MMDBNumber):
    class_name = 'Float'


class Double(MMDBNumber):
    class_name = 'Double'


class SearchTreeNode(object):
    def __init__(self, left, right):
        self.left = left
        self.right = right


class SearchTreeLeaf(object):
    def __init__(self, value):
        self.value = value
        self.str_value = str(value)
