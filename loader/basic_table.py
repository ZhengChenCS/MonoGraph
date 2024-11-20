import pandas as pd
import numpy as np
import networkx as nx
import torch
from torch import Tensor
import dgl



class BasicTable:
    def __init__(self, path):
        self.df = pd.read_csv(path, sep='|', mangle_dupe_cols=True)
        self.header = self.df.columns.tolist()
        self.indexed_columns = set()

    def get_df(self):
        return self.df

    def get_column_names(self):
        return self.header
    
    def get_column_data(self, column_name):
        return self.df[column_name].to_numpy()
    
    def get_column_data_tensor(self, column_name):
        return torch.tensor(self.df[column_name].tolist())

    def create_index(self, column_name):
        if isinstance(column_name, list):
            for col in column_name:
                if col not in self.df.columns:
                    raise ValueError(f"Column {col} does not exist in the DataFrame.")
            self.df.set_index(column_name, inplace=True)
            self.indexed_columns.update(column_name)
        elif isinstance(column_name, str):
            if column_name not in self.df.columns:
                raise ValueError(f"Column {column_name} does not exist in the DataFrame.")
            self.df.set_index(column_name, inplace=True)
            self.indexed_columns.add(column_name)
        else:
            raise TypeError("column_name must be a string or a list of strings.")
    
    def get_data_by_index(self, index, index_column, columns=None):
        if index_column not in self.indexed_columns:
            raise ValueError(f"Column {index_column} is not indexed. Please index it before accessing.")
        
        # result = self.df[self.df[index_column] == index]
        # result = self.df.xs(index, level=index_column)
        if isinstance(self.df.index, pd.MultiIndex):
            result = self.df.xs(index, level=index_column)
        else:
            result = self.df.loc[index]
        if result.empty:
            raise ValueError(f"Index {index} does not exist in the DataFrame for column {index_column}.")
        
        if columns:
            if isinstance(columns, str):
                columns = [columns]
            index_columns = result.index.names
            output = {}
            for col in columns:
                if col in index_columns:
                    output[col] = result.index.get_level_values(col).tolist()
                else:
                    output[col] = result[col].tolist()
            return output
        return result

    def reorder_table(self, indices):
        if not isinstance(indices, list):
            raise TypeError("Indices must be a list.")
        current_index = set(self.df.index)

        if not set(indices).issubset(current_index):
            raise ValueError("Indices contain elements not in the DataFrame's index.")

        missing_indices = list(current_index - set(indices))

        full_indices = indices + missing_indices
        self.df = self.df.loc[full_indices].reset_index(drop=True)
    
    def save(self, path):
        self.df.to_csv(path, sep='|', index=True)

    def __str__(self):
        return f"BasicTable with {len(self.df.columns)} columns: {self.df.columns.tolist()}"

class EdgeTable(BasicTable):
    def __init__(self, path, src_column_name, dst_column_name, graph_type='heterogeneous'):
        super().__init__(path)
        self.graph = nx.DiGraph()
        self.src_column_name = src_column_name
        self.dst_column_name = dst_column_name
        self.graph_type = graph_type
        self._create_graph()

    def _create_graph(self):
        if self.graph_type == 'heterogeneous':
            self.df['src'] = 'src_' + self.df[self.src_column_name].astype(str)
            self.df['dst'] = 'dst_' + self.df[self.dst_column_name].astype(str)
            source_col = 'src'
            target_col = 'dst'
        else:
            source_col = self.src_column_name
            target_col = self.dst_column_name

        self.graph = nx.from_pandas_edgelist(
            self.df,
            source=source_col,
            target=target_col,
            edge_attr=True,
            create_using=nx.DiGraph()
        )

    def get_src_nodes(self):
        if self.graph_type == 'heterogeneous':
            src_nodes = [node for node in self.graph.nodes if node.startswith('src_')]
            return src_nodes
            # return [int(node.split('_')[1]) for node in src_nodes]
        else:
            return list(self.df[self.src_column_name].unique())
    
    def get_dst_nodes(self):
        if self.graph_type == 'heterogeneous':
            dst_nodes = [node for node in self.graph.nodes if node.startswith('dst_')]
            return dst_nodes
            # return [int(node.split('_')[1]) for node in dst_nodes]
        else:
            return list(self.df[self.dst_column_name].unique())
    
    
    def get_neighbors(self, node):
        if self.graph_type == 'heterogeneous':
            if node.startswith('src_'):
                return list(self.graph.neighbors(node))
            elif node.startswith('dst_'):
                return list(self.graph.predecessors(node))
            else:
                raise ValueError("Node must start with 'src_' or 'dst_'.")
        else:
            return list(self.graph.neighbors(node))
        

    def expand(self, input, direction='out'):
        if isinstance(input, (np.ndarray, Tensor)):
            input = input.tolist()
        elif not isinstance(input, list):
            raise TypeError("Input must be a list, numpy array, or Tensor.")

        neighbors_list = []

        for node in input:
            if self.graph_type == 'heterogeneous':
                node = f"src_{node}" if direction == 'out' else f"dst_{node}"

            if node in self.graph:
                if direction == 'out':
                    node_neighbors = list(self.graph.neighbors(node))
                elif direction == 'in':
                    node_neighbors = list(self.graph.predecessors(node))
                else:
                    raise ValueError("Direction must be 'out' or 'in'.")
                cleaned_neighbors = [int(neighbor.split('_')[1]) for neighbor in node_neighbors]
                neighbors_list.append(cleaned_neighbors)
            else:
                neighbors_list.append([])

        return neighbors_list

    def __str__(self):
        return f"EdgeTable with {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} edges"


