import pandas as pd
import numpy as np
import torch
from torch_sparse import SparseTensor
from torch import Tensor



class BasicTable:
    def __init__(self, path):
        self.df = pd.read_csv(path, sep='|')
        self.header = self.df.columns.tolist()

    def get_df(self):
        return self.df
    
    def get_column_names(self):
        return self.header
    
    def get_column_data(self, column_name):
        return self.df[column_name].tolist()
    
    def get_column_data_tensor(self, column_name):
        return torch.tensor(self.df[column_name].tolist())
    
    def __str__(self):
        return f"BasicTable with {len(self.df.columns)} columns: {self.df.columns.tolist()}"

class BiGraph():
    def __init__(self, src: Tensor, dst: Tensor, **edge_attr):
        # first generate unique id for src
        self.src = src
        self.dst = dst
        self.edge_attr = edge_attr
        unique_src, src_inverse_indices = torch.unique(src, return_inverse=True)
        unique_dst, dst_inverse_indices = torch.unique(dst, return_inverse=True)
        pass