import sys
import glob
import os
import fnmatch
import re
import os 
import sys
import time
import pickle

# import graph_bind
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(monograph_dir)


from storage.basic_table import BasicTable
from storage.edge_table import EdgeTable
from storage.vertex_table import VertexTable
from storage.transform_graph import transformed_graph

import libmono

mono = libmono.T_Graph("VertexTest")

path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvBasic-StringDateFormatter"

table_path = f"{path}/dynamic/comment_0_0.csv"
# table_path = f"{path}/static/place_0_0.csv"
start_time = time.time()
table = VertexTable(table_path, "comment")
table.create_index('id')
print(table)
print(table.df.dtypes)
end_time = time.time()
print(f"Load comment table finished in {end_time - start_time:.2f} seconds")

print("start transform comment table...")
start_time = time.time()
mono.transformVertexTable(table)
end_time = time.time()
print(f"Transform comment table finished in {end_time - start_time:.2f} seconds")



