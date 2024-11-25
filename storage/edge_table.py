import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(monograph_dir)
from storage.basic_table import BasicTable
import networkx as nx
import numpy as np
from storage.graph import Graph
from torch import Tensor

class EdgeTable(BasicTable):
    def __init__(self, path, src_column_name, dst_column_name, graph_type='heterogeneous'):
        super().__init__(path)
        self.graph = None
        self.src_column_name = src_column_name
        self.dst_column_name = dst_column_name
        self.graph_type = graph_type
        self.src_id_map = {}
        self.dst_id_map = {}
        self.src_array = None
        self.dst_array = None
        self.src_id_reverse_map = None
        self.dst_id_reverse_map = None
        self._create_graph()

    def _create_graph(self):
        unique_src = self.df[self.src_column_name].unique()
        unique_dst = self.df[self.dst_column_name].unique()

        self.src_id_map = {value: idx for idx, value in enumerate(unique_src)}
        self.dst_id_map = {value: idx for idx, value in enumerate(unique_dst)}

        self.src_array = np.array([self.src_id_map[value] for value in self.df[self.src_column_name]])
        self.dst_array = np.array([self.dst_id_map[value] for value in self.df[self.dst_column_name]])
        self.graph = Graph(self.src_array, self.dst_array)

        self.src_id_reverse_map = np.array(unique_src)
        self.dst_id_reverse_map = np.array(unique_dst)

    def get_src_nodes(self):
        return list(self.src_id_map.keys())

    def get_dst_nodes(self):
        return list(self.dst_id_map.keys())

    def expand(self, input, direction='out'):
        if isinstance(input, (np.ndarray, Tensor)):
            input = input.tolist()
        elif not isinstance(input, list):
            raise TypeError("Input must be a list, numpy array, or Tensor.")

        if direction == 'out':
            compact_ids = [self.src_id_map[original_node] for original_node in input if original_node in self.src_id_map]
        elif direction == 'in':
            compact_ids = [self.dst_id_map[original_node] for original_node in input if original_node in self.dst_id_map]
        else:
            raise ValueError("Direction must be 'out' or 'in'.")
        neighbors, degrees = self.graph.get_batch_neighbors(compact_ids, direction)
        if direction == 'out':
            original_neighbors = [self.dst_id_reverse_map[n] for n in neighbors]
        elif direction == 'in':
            original_neighbors = [self.src_id_reverse_map[n] for n in neighbors]
        neighbors_list = []
        index = 0
        for degree in degrees:
            neighbors_list.append(original_neighbors[index:index + degree])
            index += degree
        return neighbors_list

    def __str__(self):
        return f"EdgeTable with {len(self.src_array)} edges"


