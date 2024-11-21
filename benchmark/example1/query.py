import glob
import os
import fnmatch
import re
import os 
import sys
import time
import pickle
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
sys.path.append(monograph_dir)

from dataset.ldbc import LDBC


# place_id -> person_id -> comment_id -> creationDate, locationIP, browserUsed, content
def test_query(ldbc, place_id):

    start_time = time.time()
    person_filter_start_time = time.time()
    # Get all person ids who belong to place with id = place_id
    person_table = ldbc.get_table("Person")
    person_ids_in_place = person_table.get_data_by_index(index=place_id, index_column='place', columns=['id'])['id']
    
    person_filter_end_time = time.time()
    print(f"Filtering person ids took {person_filter_end_time - person_filter_start_time:.4f} seconds")
    # print(person_ids_in_place)

    # Get all comment ids liked by person_ids_in_place
    person_like_comment_table = ldbc.edge_table["Person_likes_Comment"]
    active_comment_id = person_like_comment_table.expand(person_ids_in_place)
    expand_time = time.time()
    print(f"Expanding person likes took {expand_time - person_filter_end_time:.4f} seconds")

    active_comment_id = [item for sublist in active_comment_id for item in sublist]
    # with open(f"../inter_result/comment_{place_id}.pkl", "wb") as f:
    #     pickle.dump(active_comment_id, f)

    result_start_time = time.time()
    comment_table = ldbc.get_table("Comment")
    # get result data from comment table
    result_names = ["creationDate", "locationIP", "browserUsed", "content"]
    result_data = comment_table.get_data_by_index(index=active_comment_id, index_column='id', columns=result_names)
    result_end_time = time.time()
    print(f"Getting result data took {result_end_time - result_start_time:.4f} seconds")

    print(len(result_data["creationDate"]))
    total_time = time.time()
    print(f"Total execution time: {total_time - start_time:.4f} seconds")

if __name__ == "__main__":
    start_time = time.time()
    
    # Load LDBC data
    ldbc = LDBC("/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter")

    load_time = time.time()
    print(f"Data loading and edge table building took {load_time - start_time:.4f} seconds")

    place_ids = [1356, 1353, 519, 918, 211, 264, 512, 129, 280, 132]

    for place_id in place_ids:
        test_query(ldbc, place_id)
        # break
    
    