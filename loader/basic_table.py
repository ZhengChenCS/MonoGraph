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
        return self.df[column_name].to_numpy()
    
    def get_column_data_tensor(self, column_name):
        return torch.tensor(self.df[column_name].tolist())
    
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

# path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_studyAt_organisation_0_0.csv"
# table = BasicTable(path)
# src = table.get_df().iloc[:, 0].to_numpy()
# dst = table.get_df().iloc[:, 1].to_numpy()
# edge_table = EdgeTable(torch.tensor(src), torch.tensor(dst))
# input = torch.Tensor([65, 94, 102])
# rowptr, output, e_id = edge_table.expand(input)
# print(rowptr)
# print(output)
# print(e_id)