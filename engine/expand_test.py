import sys
sys.path.append("..")
from loader.ldbc import LDBC
import torch
from torch_scatter import gather_csr, scatter, segment_csr
from torch_sparse import SparseTensor


if __name__ == "__main__":
    ldbc = LDBC("/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter")
    ldbc.load_data()
    ldbc.build_edge_table()

    place_id = 1356

    person_table = ldbc.get_table("Person")
    person_like_comment_table = ldbc.get_table("Person_likes_Comment")

    # get all person ids who belong to place with id = place_id
    person_ids = person_table.get_column_data_tensor('id')
    place_ids = person_table.get_column_data_tensor('place')
    person_ids_in_place = person_ids[place_ids == place_id]
    print(person_ids_in_place)

    # get all comment ids liked by person_ids_in_place
    person_like_comment_table = ldbc.edge_table["Person_likes_Comment"]
    rowptr, output, e_id = person_like_comment_table.expand(person_ids_in_place)
    print(rowptr)
    print(output)
    print(e_id)