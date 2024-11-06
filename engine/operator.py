import torch
from torch import Tensor
from torch_scatter import gather_csr, scatter, segment_csr
from torch_sparse import SparseTensor


def expand(edge_index: SparseTensor, input: Tensor):
    rowptr, col, _ = edge_index.csr()
    neighborCounts = torch.zeros_like(input)

    

