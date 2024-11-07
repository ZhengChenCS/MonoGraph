import pandas as pd
import numpy as np
import torch
from torch_sparse import SparseTensor
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
    

    def __str__(self):
        return f"BasicTable with {len(self.df.columns)} columns: {self.df.columns.tolist()}"

class EdgeTable:
    def __init__(self, src: Tensor, dst: Tensor, **edge_attr):
        # first generate unique id for src
        unique_src, src_inverse_indices = torch.unique(src, return_inverse=True)
        unique_dst, dst_inverse_indices = torch.unique(dst, return_inverse=True)
        self.src_node_map = unique_src
        self.dst_node_map = unique_dst
        self.src = src_inverse_indices
        self.dst = dst_inverse_indices
        self.edge_attr = edge_attr
        self.src_name = None
        self.dst_name = None

        self.sp = SparseTensor(row=self.src, col=self.dst, value=None, 
                               sparse_sizes=(self.src.max().item()+1, self.dst.max().item()+1))
    
    def expand(self, input: Tensor, num_neighbors: int = 1000000000, replace: bool = False):
        # first find the unique id of input
        indices = torch.nonzero(self.src_node_map.unsqueeze(0) == input.unsqueeze(1), as_tuple=True)[1]
        vaild_input = indices[indices >= 0]
        rowptr, col, _ = self.sp.csr()
        rowptr, col, n_id, e_id = torch.ops.torch_sparse.sample_adj(
        rowptr, col, vaild_input, num_neighbors, replace)
        # out = SparseTensor(rowptr=rowptr, row=None, col=col, value=None, sparse_sizes=(input.size()[0], n_id.size(0)), is_sorted=True)

        # return expanded graph
        real_col = self.dst_node_map[col]
        return rowptr, real_col, e_id

# path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter/dynamic/comment_0_0.csv"
# table = BasicTable(path)
# table.create_hash_index('id')
# # input = torch.tensor([709926])
# input = [709926]
# print(table.get_indices_by_key('id', input))