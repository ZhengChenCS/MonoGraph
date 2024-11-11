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

def test_scan(table, indices, result_names):
    start_time = time.time()
    result_data = {}
    for result_name in result_names:
        result_data[result_name] = table.get_data_by_indices(result_name, indices)
    end_time = time.time()
    print(f"Scanning took {end_time - start_time:.4f} seconds")
    return result_data


if __name__ == "__main__":
    comment_path = "/mnt/nvme/ldbc_dataset/social_network-sf30-CsvCompositeMergeForeign-LongDateFormatter/dynamic/comment_0_0.csv"
    comment_table = BasicTable(comment_path)
    
    # read from disk to memory comment indices
    with open("comment_indices_30.pkl", "rb") as f:
        comment_indices = pickle.load(f)
    # print(comment_indices)

    place_ids = [1356, 1353, 519, 918, 211, 264, 512, 129, 280, 132]

    for place_id in place_ids:
        path = f"comment_indices_{place_id}.pkl"
        with open(path, "rb") as f:
            comment_indices = pickle.load(f)
        test_scan(comment_table, comment_indices, ["creationDate", "locationIP", "browserUsed", "content"])
    

    # random_access_start = time.time()
    # for result_name in result_names:
    #     result_data[result_name] = comment_table.get_data_by_indices(result_name, comment_indices)
    # random_access_end = time.time()
    # print(f"Random access took {random_access_end - random_access_start:.4f} seconds")

    # sorted_access_start = time.time()
    # sorted_indices = np.sort(comment_indices)
    # for result_name in result_names:
    #     result_data[result_name] = comment_table.get_data_by_indices(result_name, sorted_indices)
    # sorted_access_end = time.time()
    # print(f"Sorted access took {sorted_access_end - sorted_access_start:.4f} seconds")

    # sequential_access_start = time.time()
    # sequential_indices = np.arange(100, len(comment_indices))
    # for result_name in result_names:
    #     result_data[result_name] = comment_table.get_data_by_indices(result_name, sequential_indices)
    # sequential_access_end = time.time()
    # print(f"Sequential access took {sequential_access_end - sequential_access_start:.4f} seconds")

    
