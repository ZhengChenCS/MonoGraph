import pandas as pd
import numpy as np
import networkx as nx
import torch
from torch import Tensor



class BasicTable:
    def __init__(self, path):
        self.df = pd.read_csv(path, sep='|')
        self.header = self.df.columns.tolist()
        self.index = {}

    def get_df(self):
        return self.df
    
    def get_column_names(self):
        return self.header
    
    def get_column_data(self, column_name):
        return self.df[column_name].to_numpy()
    
    def get_column_data_tensor(self, column_name):
        return torch.tensor(self.df[column_name].tolist())
    
    def create_continuous_index(self, column_name):
        column_data = self.df[column_name].to_numpy()
        min_value = column_data.min()
        max_value = column_data.max()
        index_array = np.full((max_value - min_value + 1,), -1, dtype=int)
        for idx, value in enumerate(column_data):
            index_array[value - min_value] = idx
        self.index[column_name] = (index_array, min_value)

    def get_continuous_index(self, column_name, value):
        index_array, min_value = self.index[column_name]
        return index_array[value - min_value]

    def create_hash_index(self, column_name):
        self.index[column_name] = {value: idx for idx, value in enumerate(self.df[column_name])}
    
    def get_unique_index(self, column_name):
        return self.index[column_name]
    
    def get_data_by_indices(self, column_name, indices):
        return self.df[column_name].to_numpy()[indices]
    
    def get_indices_by_key(self, column_name, key):
        if column_name not in self.index:
            raise ValueError(f"Column {column_name} does not have a index")
        else:
            if isinstance(key, Tensor):
                key = key.tolist()
            indices = []
            for k in key:
                if k not in self.index[column_name]:    
                    raise ValueError(f"Key {k} does not exist in column {column_name}")
                else:
                    indices.append(self.index[column_name][k])
            return indices
    
    def reorder_table(self, indices):
        self.df = self.df.iloc[indices]
    

    def __str__(self):
        return f"BasicTable with {len(self.df.columns)} columns: {self.df.columns.tolist()}"

class EdgeTable(BasicTable):
    def __init__(self, path, src_column_name, dst_column_name):
        super().__init__(path)
        self.graph = nx.DiGraph()
        self.src_column_name = src_column_name
        self.dst_column_name = dst_column_name
        self._create_graph()

    def _create_graph(self):
        src = self.df[self.src_column_name].tolist()
        dst = self.df[self.dst_column_name].tolist()
        
        edge_attrs = self.df.drop(columns=[self.src_column_name, self.dst_column_name]).to_dict(orient='records')
        
        for s, d, attr in zip(src, dst, edge_attrs):
            self.graph.add_edge(s, d, **attr)

    def expand(self, input, direction='out'):
        pass

    def __str__(self):
        return f"EdgeTable with {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} edges"

