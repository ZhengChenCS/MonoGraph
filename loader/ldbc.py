import glob
import os
import fnmatch
import re
import torch
import time
from loader.basic_table import BasicTable, EdgeTable

class LDBC:
    def __init__(self, path):
        self.parent_path = path
        self.static_table_name = ['Organisation',  'Place', 'Tag', 'TagClass']
        self.dynamic_table_name = ['Comment', 'Person', 'Person_studyAt_organisation', 'Comment_hasTag_Tag', 
        'Person_hasInterest_Tag',  'Person_workAt_organisation', 
        'Forum', 'Person_knows_Person', 'Post', 'Forum_hasMember_Person',  'Person_likes_Comment', 'Post_hasTag_Tag',
        'Forum_hasTag_Tag', 'Person_likes_Post']
        self.vertex_table_name = ['Comment', 'Person', 'Forum', 'Post']
        self.edge_table_name = [
            ('Person_studyAt_organisation', 'Person.id', 'Organisation.id'),
            ('Comment_hasTag_Tag', 'Comment.id', 'Tag.id'),
            ('Person_hasInterest_Tag', 'Person.id', 'Tag.id'),
            ('Person_workAt_organisation', 'Person.id', 'Organisation.id'),
            ('Person_knows_Person', 'Person.id', 'Person.id.1'),
            ('Forum_hasMember_Person', 'Forum.id', 'Person.id'),
            ('Person_likes_Comment', 'Person.id', 'Comment.id'),
            ('Post_hasTag_Tag', 'Post.id', 'Tag.id'),
            ('Forum_hasTag_Tag', 'Forum.id', 'Tag.id'),
            ('Person_likes_Post', 'Person.id', 'Post.id')
        ]
        self.table = {}
        self.edge_table = {}
        
        total_start_time = time.time()
        
        load_start_time = time.time()
        self._load_data()
        load_end_time = time.time()
        print(f'Data loading time: {load_end_time - load_start_time:.2f} seconds')
        
        edge_start_time = time.time()
        self._build_edge_table()
        edge_end_time = time.time()
        print(f'Edge table building time: {edge_end_time - edge_start_time:.2f} seconds')
        
        index_start_time = time.time()
        self._build_index()
        index_end_time = time.time()
        print(f'Index building time: {index_end_time - index_start_time:.2f} seconds')
        
        total_end_time = time.time()
        print(f'Total load time: {total_end_time - total_start_time:.2f} seconds')
    
    def _load_data(self):
        for table_name in self.static_table_name:
            files = glob.glob(os.path.join(self.parent_path, "static", '*'))
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                # print(f"Processing file: {file}")
                table = BasicTable(file)
                self.table[table_name] = table
        for table_name in self.vertex_table_name:
            files = glob.glob(os.path.join(self.parent_path, "dynamic", '*'))
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                # print(f"Processing file: {file}")
                table = BasicTable(file)
                self.table[table_name] = table
        
    
    def _build_edge_table(self):
        for table_name, src_column, dst_column in self.edge_table_name:
            files = glob.glob(os.path.join(self.parent_path, "dynamic", '*'))
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                # print(f"Processing file: {file}")
                if table_name == 'Person_knows_Person':
                    edge_table = EdgeTable(file, src_column, dst_column, graph_type='homogeneous')
                else:
                    edge_table = EdgeTable(file, src_column, dst_column)
                self.edge_table[table_name] = edge_table
    
    def _build_index(self):
        self.table['TagClass'].create_index('id')
        self.table['Person'].create_index(['id', 'place'])
        self.table['Comment'].create_index('id')
        self.table['Forum'].create_index('id')
        self.table['Post'].create_index('id')
    

    def get_table(self, table_name):
        return self.table[table_name]
    
    def get_edge_table(self, table_name):
        return self.edge_table[table_name]
    
    # reorder the table by the order of the indices
    def reorder_table(self, table_name, indices):
        self.table[table_name].reorder_table(indices)

# path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter"
# ldbc = LDBC(path)
# ldbc.load_data()
# print(ldbc.get_table("Person"))


