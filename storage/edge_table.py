import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(monograph_dir)
from storage.basic_table import BasicTable
import networkx as nx
import numpy as np
from torch import Tensor

class EdgeTable(BasicTable):
    def __init__(self, path, src_column_name, dst_column_name, graph_type='heterogeneous'):
        super().__init__(path)
        self.graph = nx.DiGraph()
        self.src_column_name = src_column_name
        self.dst_column_name = dst_column_name
        self.graph_type = graph_type
        self.src_id_map = {}
        self.dst_id_map = {}
        self.src_array = None  # 用于存储 src 的 NumPy 数组
        self.dst_array = None  # 用于存储 dst 的 NumPy 数组
        self._create_graph()

    def _create_graph(self):
        unique_src = self.df[self.src_column_name].unique()
        unique_dst = self.df[self.dst_column_name].unique()

        self.src_id_map = {value: idx for idx, value in enumerate(unique_src)}
        self.dst_id_map = {value: idx for idx, value in enumerate(unique_dst)}

        self.src_array = np.array([self.src_id_map[value] for value in self.df[self.src_column_name]])
        self.dst_array = np.array([self.dst_id_map[value] for value in self.df[self.dst_column_name]])

    def get_src_nodes(self):
        # 返回原始 src 节点
        return list(self.src_id_map.keys())

    def get_dst_nodes(self):
        # 返回原始 dst 节点
        return list(self.dst_id_map.keys())

    def expand(self, input, direction='out'):
        if isinstance(input, (np.ndarray, Tensor)):
            input = input.tolist()
        elif not isinstance(input, list):
            raise TypeError("Input must be a list, numpy array, or Tensor.")

        neighbors_list = []

        for original_node in input:
            # 将原始 ID 映射到紧凑 ID
            if direction == 'out':
                if original_node not in self.src_id_map:
                    neighbors_list.append([])
                    continue
                node = self.src_id_map[original_node]
                node_neighbors = self.dst_array[self.src_array == node]
                original_neighbors = [list(self.dst_id_map.keys())[list(self.dst_id_map.values()).index(n)] for n in node_neighbors]
            elif direction == 'in':
                if original_node not in self.dst_id_map:
                    neighbors_list.append([])
                    continue
                node = self.dst_id_map[original_node]
                node_neighbors = self.src_array[self.dst_array == node]
                original_neighbors = [list(self.src_id_map.keys())[list(self.src_id_map.values()).index(n)] for n in node_neighbors]
            else:
                raise ValueError("Direction must be 'out' or 'in'.")

            neighbors_list.append(original_neighbors)

        return neighbors_list

    def __str__(self):
        return f"EdgeTable with {len(self.src_array)} edges"


