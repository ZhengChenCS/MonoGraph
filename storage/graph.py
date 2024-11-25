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
    
    def label_propagation(self, src_labels, dst_labels, direction='out'):
        if not isinstance(src_labels, torch.Tensor):
            src_labels = torch.tensor(src_labels)
        if not isinstance(dst_labels, torch.Tensor):
            dst_labels = torch.tensor(dst_labels)
        
        if direction == 'out':
            label_matrix = torch.zeros((len(dst_labels), src_labels.max() + 1), dtype=torch.int)
            label_matrix.scatter_add_(1, src_labels.unsqueeze(1), torch.ones_like(src_labels).unsqueeze(1))
            propagated_labels = self.graph.spmm(label_matrix)

            new_dst_labels = propagated_labels.argmax(dim=1)

        elif direction == 'in':
            label_matrix = torch.zeros((len(src_labels), dst_labels.max() + 1), dtype=torch.int)
            label_matrix.scatter_add_(1, dst_labels.unsqueeze(1), torch.ones_like(dst_labels).unsqueeze(1))
            propagated_labels = self.graph.t().spmm(label_matrix)
            new_src_labels = propagated_labels.argmax(dim=1)

        else:
            raise ValueError("Direction must be 'out' or 'in'.")

        if direction == 'out':
            return src_labels, new_dst_labels
        else:
            return new_src_labels, dst_labels

# src = np.array([0, 0, 1, 2, 3, 3, 4, 4, 5, 6, 7, 8, 9])
# dst = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3])
# graph = Graph(src, dst)
# print(graph.get_neighbors(0))
# print(graph.get_batch_neighbors(torch.tensor([0, 1, 2]), 'out'))    