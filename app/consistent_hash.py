# app/consistent_hash.py
import hashlib
import bisect

class ConsistentHash:
    """
    一致性哈希算法实现
    """
    def __init__(self, nodes=None, replicas=3):
        """
        初始化
        :param nodes: 初始节点列表
        :param replicas: 每个物理节点的虚拟节点数量
        """
        self.replicas = replicas
        self.ring = dict()
        self.sorted_keys =
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        """
        向环中添加一个节点及其虚拟节点
        """
        for i in range(self.replicas):
            # 为每个虚拟节点生成唯一的标识符
            virtual_node_key = f"{node}:{i}"
            # 计算哈希值
            key = self._hash(virtual_node_key)
            self.ring[key] = node
            # 将哈希值插入到有序列表中
            bisect.insort(self.sorted_keys, key)

    def remove_node(self, node):
        """
        从环中移除一个节点及其所有虚拟节点
        """
        for i in range(self.replicas):
            virtual_node_key = f"{node}:{i}"
            key = self._hash(virtual_node_key)
            del self.ring[key]
            # 二分查找并移除
            index = bisect.bisect_left(self.sorted_keys, key)
            if index < len(self.sorted_keys) and self.sorted_keys[index] == key:
                self.sorted_keys.pop(index)


    def get_node(self, key_string):
        """
        根据给定的key，查找其应属的节点
        """
        if not self.ring:
            return None
        key = self._hash(key_string)
        # 在有序列表中查找第一个大于等于key哈希值的虚拟节点
        index = bisect.bisect(self.sorted_keys, key)
        # 如果索引超出范围，说明key的哈希值大于所有虚拟节点，
        # 此时应由环上的第一个节点处理（环形结构）
        if index == len(self.sorted_keys):
            index = 0
        return self.ring[self.sorted_keys[index]]

    def _hash(self, key):
        """
        使用MD5计算哈希值，并返回一个整数
        """
        md5_hash = hashlib.md5(key.encode('utf-8')).hexdigest()
        return int(md5_hash, 16)