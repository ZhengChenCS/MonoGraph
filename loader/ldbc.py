from loader.basic_table import BasicTable, EdgeTable
import glob
import os
import fnmatch
import re
import torch

class LDBC:
    def __init__(self, path):
        self.parent_path = path
        self.static_table_name = ['Organisation',  'Place', 'Tag', 'TagClass']
        self.dynamic_table_name = ['Comment', 'Person', 'Person_studyAt_organisation', 'Comment_hasTag_Tag', 
        'Person_hasInterest_Tag',  'Person_workAt_organisation', 
        'Forum', 'Person_knows_Person', 'Post', 'Forum_hasMember_Person',  'Person_likes_Comment', 'Post_hasTag_Tag',
        'Forum_hasTag_Tag', 'Person_likes_Post']
        self.vertex_table_name = ['Comment', 'Person', 'Forum', 'Post']
        self.edge_table_name = ['Person_studyAt_organisation', 'Comment_hasTag_Tag', 
        'Person_hasInterest_Tag', 'Person_workAt_organisation',
        'Person_knows_Person', 'Forum_hasMember_Person', 'Person_likes_Comment',
        'Post_hasTag_Tag', 'Forum_hasTag_Tag', 'Person_likes_Post'
        ]
        self.table = {}
        self.edge_table = {}
    
    def load_data(self):
        for table_name in self.static_table_name:
            files = glob.glob(os.path.join(self.parent_path, "static", '*'))
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                # print(f"Processing file: {file}")
                table = BasicTable(file)
                self.table[table_name] = table
        
        for table_name in self.dynamic_table_name:
            files = glob.glob(os.path.join(self.parent_path, "dynamic", '*'))
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                # print(f"Processing file: {file}")
                table = BasicTable(file)
                self.table[table_name] = table
    
    def build_edge_table(self):
        for table_name in self.edge_table_name:
            table = self.table[table_name]
            src = table.get_df().iloc[:, 0].to_numpy()
            dst = table.get_df().iloc[:, 1].to_numpy()
            edge_table = EdgeTable(torch.tensor(src), torch.tensor(dst))
            self.edge_table[table_name] = edge_table
    
    def build_index(self):
        self.table['TagClass'].create_continuous_index('id')
        self.table['Person'].create_hash_index('placeId')
        self.table['Comment'].create_hash_index('id')
        self.table['Person'].create_hash_index('id')
        self.table['Forum'].create_hash_index('id')
        self.table['Post'].create_hash_index('id')
    

    def get_table(self, table_name):
        return self.table[table_name]
    
    # reorder the table by the order of the indices
    def reorder_table(self, table_name, indices):
        pass

# path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter"
# ldbc = LDBC(path)
# ldbc.load_data()
# print(ldbc.get_table("Person"))


