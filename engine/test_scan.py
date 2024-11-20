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
    result_data = table.get_data_by_index(index=indices, index_column='id', columns=result_names)
    end_time = time.time()
    print(f"Scanning took {end_time - start_time:.4f} seconds")
    return result_data


if __name__ == "__main__":
    # comment_path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter/dynamic/comment_0_0.csv"
    comment_path = "../inter_result/comment_0_0.csv"
    comment_table = BasicTable(comment_path)
    comment_table.create_index('id')
    
    # read from disk to memory comment indices
    # with open("comment_indices_30.pkl", "rb") as f:
    #     comment_indices = pickle.load(f)
    # print(comment_indices)

    place_ids = [1353,1356, 519, 918, 211, 264, 512, 129, 280, 132]

    for place_id in place_ids:
        path = f"../inter_result/comment_{place_id}.pkl"
        with open(path, "rb") as f:
            comment_indices = pickle.load(f)
        result_data = test_scan(comment_table, comment_indices, ["creationDate", "locationIP", "browserUsed", "content"])
        # break


    
