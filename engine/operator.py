import torch
from torch import Tensor
import pandas as pd

def get_indices_sort_join(input: Tensor, target: Tensor, is_sorted=False):
    if is_sorted:
        indices = torch.searchsorted(target, input)
        mask = target[indices] == input
        valid_indices = indices[mask]
    else:
        sorted_target, sorted_indices = torch.sort(target)
        indices = torch.searchsorted(sorted_target, input)
        mask = sorted_target[indices] == input
        valid_indices = sorted_indices[indices[mask]]
    
    return valid_indices

def get_indices_hash_join(input: Tensor, target: Tensor):
    input_df = pd.DataFrame({'value': input.numpy()})
    target_df = pd.DataFrame({'value': target.numpy()})
    merged_df = pd.merge(target_df.reset_index(), input_df, on='value', how='inner')
    valid_indices = merged_df['index'].to_numpy()

    return torch.tensor(valid_indices, dtype=torch.long)
