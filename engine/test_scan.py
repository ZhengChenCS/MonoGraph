import sys
import time
sys.path.append("..")
from loader.ldbc import LDBC
from loader.basic_table import BasicTable
import torch
from torch_scatter import gather_csr, scatter, segment_csr
from torch_sparse import SparseTensor
import pickle
import numpy as np


if __name__ == "__main__":
    comment_path = "/mnt/nvme/ldbc_dataset/social_network-sf30-CsvCompositeMergeForeign-LongDateFormatter/dynamic/comment_0_0.csv"
    comment_table = BasicTable(comment_path)
    
    # read from disk to memory comment indices
    with open("comment_indices_30.pkl", "rb") as f:
        comment_indices = pickle.load(f)
    
    result_names = ["creationDate", "locationIP", "browserUsed", "content"]
    # result_names = ["creationDate"]
    result_data = {}

    random_access_start = time.time()
    for result_name in result_names:
        result_data[result_name] = comment_table.get_data_by_indices(result_name, comment_indices)
    random_access_end = time.time()
    print(f"Random access took {random_access_end - random_access_start:.4f} seconds")

    sequential_access_start = time.time()
    sequential_indices = np.arange(0, len(comment_indices))
    for result_name in result_names:
        result_data[result_name] = comment_table.get_data_by_indices(result_name, sequential_indices)
    sequential_access_end = time.time()
    print(f"Sequential access took {sequential_access_end - sequential_access_start:.4f} seconds")
