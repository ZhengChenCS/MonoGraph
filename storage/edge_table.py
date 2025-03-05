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
import pickle

class EdgeTable(BasicTable):
    def __init__(self, path, table_name, src_column_name, dst_column_name, graph_type='heterogeneous'):
        super().__init__(path, table_name)
        self.graph = None # 边表所对应的原图，src->des
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
    
    def transform(self, t_graph):
        """
        transform edge table into transformed graph

        Args: 
            t_graph (transformed_graph_meta): basic info of transformed graph
        """
        print("transforming edge table", self.name, flush=True)
        # cols = self.df.columns.tolist()
        col_names = self.header
        table_name = self.name

        # # Checkpoint save before the loop
        # checkpoint_path = f'{self.name}_checkpoint.pkl'
        
        # try:
        #     with open(checkpoint_path, 'rb') as checkpoint_file:
        #         t_graph = pickle.load(checkpoint_file)
        #         print("Checkpoint loaded successfully.")
        # except FileNotFoundError:
        #     print("No checkpoint found. Starting from scratch.")

        t_graph.create_vertex(self.table_encoding(table_name))
        table_id = t_graph.get_id(self.table_encoding(table_name))

        num_rows = len(self.df)
        num_cols = len(col_names)

        # src_index = self.df.get_loc(self.src_column_name)
        # dst_index = self.df.get_loc(self.des_column_name)
        t_graph.create_vertex(self.table_encoding(self.src_column_name))
        t_graph.create_vertex(self.table_encoding(self.dst_column_name))
        src_table_id = t_graph.get_id(self.table_encoding(self.src_column_name))
        dst_table_id = t_graph.get_id(self.table_encoding(self.dst_column_name))
        for i in range(num_cols):
            t_graph.create_vertex(self.column_encoding(col_names[i], table_id))
            col_name_id = t_graph.get_id(self.column_encoding(col_names[i], table_id))
            t_graph.create_edge_mapping(col_name_id, table_id)
        for i in range(num_rows):
            t_graph.create_vertex(self.edge_id_encoding(i, table_id))
            edge_id = t_graph.get_id(self.edge_id_encoding(i, table_id))
            t_graph.create_edge_mapping(edge_id, table_id)
        ## create edge prop value (not including src and dst)
        for cur in range(num_rows):
            for col_name in col_names:
                if col_name == self.src_column_name or col_name == self.dst_column_name:
                    continue
                else:
                    data = self.df.iloc[cur][col_name]
                    t_graph.create_vertex(data) # create value
                    value_id = t_graph.get_id(data)
                    t_graph.create_edge_mapping(col_name_id, value_id)
                    t_graph.create_edge(self.edge_id_encoding(cur, table_id), data)
        ## create edge from src to dst
        for i in range(num_rows):
            edge_id = t_graph.get_id(self.edge_id_encoding(i, table_id))
            src_value = self.df.iloc[i][self.src_column_name]
            dst_value = self.df.iloc[i][self.dst_column_name]
            t_graph.create_vertex(self.primary_key_encoding(src_value, src_table_id))
            t_graph.create_vertex(self.primary_key_encoding(dst_value, dst_table_id))
            src_id = t_graph.get_id(self.primary_key_encoding(src_value, src_table_id))
            dst_id = t_graph.get_id(self.primary_key_encoding(dst_value, dst_table_id))
            t_graph.create_edge_mapping(edge_id, src_id)
            t_graph.create_edge_mapping(edge_id, dst_id)

        #     # 每经过一定数量的行后保存 checkpoint
        #     if i % 2000000 == 0:
        #         with open(checkpoint_path, 'wb') as checkpoint_file:
        #             pickle.dump(t_graph, checkpoint_file)
        #         print(f"Checkpoint saved at row {i}, temp row processed {i / num_rows * 100:.2f}%")
        
        # with open(checkpoint_path, 'wb') as checkpoint_file:
        #     pickle.dump(t_graph, checkpoint_file)
        #     print(f"Checkpoint saved after processing EdgeTable", self.name)
        return t_graph


