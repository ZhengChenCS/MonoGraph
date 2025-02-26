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
        cols = self.df.columns.tolist()
        col_name = self.header
        table_name = self.name

        t_graph.create_vertex(self.table_encoding(table_name))
        table_id = t_graph.get_id(self.table_encoding(table_name))

        num_rows = len(cols[0])
        num_cols = len(col_name)

        # table_config = {
        #     "Person_studyAt_University": {
        #         "src_table_name": "Person",
        #         "dst_table_name": "Organisation"
        #     },
        #     "Comment_hasTag_Tag": {
        #         "src_table_name": "Comment",
        #         "dst_table_name": "Tag"
        #     },
        #     "Person_hasInterest_Tag": {
        #         "src_table_name": "Person",
        #         "dst_table_name": "Tag"
        #     },
        #     "Person_workAt_Company": {
        #         "src_table_name": "Person",
        #         "dst_table_name": "Organisation"
        #     },
        #     "Person_knows_Person": {
        #         "src_table_name": "Person",
        #         "dst_table_name": "Person"
        #     },
        #     "Forum_hasMember_Person": {
        #         "src_table_name": "Forum",
        #         "dst_table_name": "Person"
        #     },
        #     "Person_likes_Comment": {
        #         "src_table_name": "Person",
        #         "dst_table_name": "Comment"
        #     },
        #     "Post_hasTag_Tag": {
        #         "src_table_name": "Post",
        #         "dst_table_name": "Tag"
        #     },
        #     "Forum_hasTag_Tag": {
        #         "src_table_name": "Forum",
        #         "dst_table_name": "Tag"
        #     },
        #     "Person_likes_Post": {
        #         "src_table_name": "Person",
        #         "dst_table_name": "Post"
        #     }
        # }
        # config = table_config.get(table_name)
        # if config:
        #     src_index = 1
        #     dst_index = 2
        #     src_table_name = config["src_table_name"]
        #     dst_table_name = config["dst_table_name"]
        # else:
        #     print(f"{table_name} not exist.")
        #     exit(0)
        src_index = self.df.get_loc(self.src_column_name)
        dst_index = self.df.get_loc(self.des_column_name)
        src_table_id = t_graph.get_id(self.table_encoding(self.src_column_name))
        dst_table_id = t_graph.get_id(self.table_encoding(self.des_column_name))
        for i in range(num_cols):
            t_graph.create_vertex(self.column_encoding(col_name[i], table_id))
            col_name_id = t_graph.get_id(self.column_encoding(col_name[i], table_id))
            t_graph.create_edge_mapping(col_name_id, table_id)
        for i in range(num_rows):
            t_graph.create_vertex(self.edge_id_encoding(i, table_id))
            edge_id = t_graph.get_id(self.edge_id_encoding(i, table_id))
            t_graph.create_edge_mapping(edge_id, table_id)
        ## create edge prop value (not including src and dst)
        for i in range(num_cols):
            if i == src_index or i == dst_index:
                continue
            col_name_id = t_graph.get_id(self.column_encoding(col_name[i], table_id))
            for j in range(num_rows):
                t_graph.create_vertex(cols[i][j]) # create value
                value_id = t_graph.get_id(cols[i][j])
                t_graph.create_edge_mapping(col_name_id, value_id)
                t_graph.create_edge(self.edge_id_encoding(j, table_id), cols[i][j])
        ## create edge from src to dst
        for i in range(num_rows):
            edge_id = t_graph.get_id(self.edge_id_encoding(i, table_id))
            src_value = cols[src_index][i]
            dst_value = cols[dst_index][i]
            src_id = t_graph.get_id(self.primary_key_encoding(src_value, src_table_id))
            dst_id = t_graph.get_id(self.primary_key_encoding(dst_value, dst_table_id))
            t_graph.create_edge_mapping(edge_id, src_id)
            t_graph.create_edge_mapping(edge_id, dst_id)
        return t_graph


