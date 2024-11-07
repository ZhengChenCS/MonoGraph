import sys
import time
sys.path.append("..")
from loader.ldbc import LDBC
import torch
from torch_scatter import gather_csr, scatter, segment_csr
from torch_sparse import SparseTensor
from engine.operator import get_indices_sort_join, get_indices_hash_join
import pickle

if __name__ == "__main__":
    start_time = time.time()
    
    # Load LDBC data
    ldbc = LDBC("/mnt/nvme/ldbc_dataset/social_network-sf30-CsvCompositeMergeForeign-LongDateFormatter")
    ldbc.load_data()
    ldbc.build_edge_table()
    ldbc.build_index()
    
    load_time = time.time()
    print(f"Data loading and edge table building took {load_time - start_time:.4f} seconds")

    start_time = time.time()
    place_id = 1356

    # Get person table
    person_table = ldbc.get_table("Person")
    person_like_comment_table = ldbc.get_table("Person_likes_Comment")

    # Get all person ids who belong to place with id = place_id
    person_ids = person_table.get_column_data_tensor('id')
    place_ids = person_table.get_column_data_tensor('place')
    person_ids_in_place = person_ids[place_ids == place_id]
    
    person_filter_time = time.time()
    print(f"Filtering person ids took {person_filter_time - load_time:.4f} seconds")
    # print(person_ids_in_place)

    # Get all comment ids liked by person_ids_in_place
    person_like_comment_table = ldbc.edge_table["Person_likes_Comment"]
    rowptr, active_comment_id, e_id = person_like_comment_table.expand(person_ids_in_place)
    
    expand_time = time.time()
    print(f"Expanding person likes took {expand_time - person_filter_time:.4f} seconds")
    # print(rowptr)
    # print(output)
    # print(e_id)

    join_start_time = time.time()
    # find all comment ids in comment table
    comment_table = ldbc.get_table("Comment")

    #comment_ids = comment_table.get_column_data_tensor('id')
    # comment_indices = get_indices_sort_join(active_comment_id, comment_ids, is_sorted=True)
    # comment_indices = get_indices_hash_join(active_comment_id, comment_ids)
    comment_indices = comment_table.get_indices_by_key('id', active_comment_id)
    join_end_time = time.time()
    with open("comment_indices_30.pkl", "wb") as f:
        pickle.dump(comment_indices, f)
    print(f"Joining took {join_end_time - expand_time:.4f} seconds")
    
    result_start_time = time.time()
    # get result data from comment table
    result_names = ["creationDate", "locationIP", "browserUsed", "content"]
    result_data = {}
    for result_name in result_names:
        result_data[result_name] = comment_table.get_data_by_indices(result_name, comment_indices)
    result_end_time = time.time()
    print(f"Getting result data took {result_end_time - join_end_time:.4f} seconds")

    # # print result data
    # for result_name in result_names:
    #     print(result_data[result_name])
    print(len(result_data["creationDate"]))

    
    total_time = time.time()
    print(f"Total execution time: {total_time - start_time:.4f} seconds")