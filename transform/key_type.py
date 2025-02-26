import os
import sys

# class key_t:
#     def __init__(self, label_id, rowid, content):
#         pass


# 编码，避免table名，列名，主键，非主键hash为同一个值
def table_encoding(key):
    return '#' + key

def column_encoding(key, table_id):
    return '$' + str(table_id) + '_' + key

def edge_id_encoding(key, table_id):
    return str(table_id) + "_" + str(key)

def primary_key_encoding(key, table_id):
    return str(table_id) + "_" + key
