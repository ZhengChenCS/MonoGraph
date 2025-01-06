import os 
import sys
import random
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(monograph_dir)
import torch
from torch import Tensor
from torch_sparse import SparseTensor
import numpy as np

class Graph:
    def __init__(self, src, dst):
        self.src = torch.tensor(src)
        self.dst = torch.tensor(dst)
        self.graph = None
        self._create_graph()
    
    def _create_graph(self):
        self.graph = SparseTensor(row=self.src, col=self.dst)
    
    def get_neighbors(self, node, direction='out'):
        if direction == 'out':
            rowptr, col, _ = self.graph.csr()
            start = rowptr[node].item()
            end = rowptr[node + 1].item()
            node_neighbors = col[start:end].numpy()
        elif direction == 'in':
            colptr, row, _ = self.graph.csc()
            start = colptr[node].item()
            end = colptr[node + 1].item()
            node_neighbors = row[start:end].numpy()
        else:
            raise ValueError("Direction must be 'out' or 'in'.")
        return node_neighbors

    def get_batch_neighbors(self, nodeset, direction='out'):
        if not isinstance(nodeset, torch.Tensor):
            nodeset = torch.tensor(nodeset)
        if direction == 'out':
            rowptr, col, _ = self.graph.csr()
            start = rowptr[nodeset].view(-1)
            end = rowptr[nodeset + 1].view(-1)
            degrees = (end - start).numpy()
            indices = torch.cat([torch.arange(s, e) for s, e in zip(start, end)])
            node_neighbors = col[indices].numpy()
        elif direction == 'in':
            colptr, row, _ = self.graph.csc()
            start = colptr[nodeset].view(-1)
            end = colptr[nodeset + 1].view(-1)
            degrees = (end - start).numpy()
            indices = torch.cat([torch.arange(s, e) for s, e in zip(start, end)])
            node_neighbors = row[indices].numpy()
        else:
            raise ValueError("Direction must be 'out' or 'in'.")
        return node_neighbors, degrees