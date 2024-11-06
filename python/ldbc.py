from basic_table import BasicTable
import glob
import os
import fnmatch
import re

class LDBC:
    def __init__(self, path):
        self.parent_path = path
        self.static_table_name = ['Organisation',  'Place', 'Tag', 'TagClass']
        self.dynamic_table_name = ['Comment', 'Person', 'Person_studyAt_University', 'Comment_hasTag_Tag', 
        'Person_hasInterest_Tag',  'Person_workAt_Company', 
        'Forum', 'Person_knows_Person', 'Post', 'Forum_hasMember_Person',  'Person_likes_Comment', 'Post_hasTag_Tag',
        'Forum_hasTag_Tag', 'Person_likes_Post']
        self.table = {}
    
    def load_data(self):
        for table_name in self.static_table_name:
            files = glob.glob(os.path.join(self.parent_path, "static", '*'))
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                print(f"Processing file: {file}")
                table = BasicTable(file)
                self.table[table_name] = table
        
        for table_name in self.dynamic_table_name:
            files = glob.glob(os.path.join(self.parent_path, "dynamic", '*'))
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                print(f"Processing file: {file}")
                table = BasicTable(file)
                self.table[table_name] = table

    def get_table(self, table_name):
        return self.table[table_name]

path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter"
ldbc = LDBC(path)
ldbc.load_data()
print(ldbc.get_table("Person"))


