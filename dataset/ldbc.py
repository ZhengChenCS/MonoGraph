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

import libmono

from storage.basic_table import BasicTable
from storage.edge_table import EdgeTable
from storage.vertex_table import VertexTable
from storage.transform_graph import transformed_graph

class LDBC:
    def __init__(self, path):
        self.parent_path = path
        self.static_table_name = ['organisation', 'place_isPartOf_place', 'tagclass_isSubclassOf_tagclass', 'organisation_isLocatedIn_place', 'tag', 'tag_hasType_tagclass', 'place', 'tagclass']
        self.dynamic_table_name = ['comment', 'forum_hasModerator_person', 'person_speaks_language', 'comment_hasCreator_person', 'forum_hasTag_tag', 'person_studyAt_organisation',  
        'comment_hasTag_tag', 'person', 'person_workAt_organisation', 'comment_isLocatedIn_place', 'person_email_emailaddress', 'post',  
        'comment_replyOf_comment', 'person_hasInterest_tag', 'post_hasCreator_person', 'comment_replyOf_post', 'person_isLocatedIn_place', 'post_hasTag_tag',  
        'forum', 'person_knows_person', 'post_isLocatedIn_place', 'forum_containerOf_post', 'person_likes_comment',  
        'forum_hasMember_person', 'person_likes_post'  
        ]
        self.vertex_table_name = ['organisation', 'tag', 'place', 'tagclass', 'comment', 'person', 'post', 'forum']
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
            ('Person_likes_Post', 'Person.id', 'Post.id'),
            ('place_isPartOf_place', 'Place.id', "Place.id"),
            ('tagclass_isSubclassOf_tagclass', 'TagClass.id', 'TagClass.id'),
            ('organisation_isLocatedIn_place', 'Organisation.id', 'Place.id'),
            ('tag_hasType_tagclass', 'Tag.id', 'TagClass.id'),
            ('forum_hasModerator_person', 'Forum.id', 'Person.id'),
            ('person_speaks_language', 'Person.id', 'language'),
            ('comment_hasCreator_person', 'Comment.id', 'Person.id'),
            ('comment_isLocatedIn_place', 'Comment.id', 'Place.id'),
            ('person_email_emailaddress', 'Person.id', 'email'),
            ('comment_replyOf_comment', 'Comment.id', 'Comment.id'),
            ('post_hasCreator_person', 'Post.id', 'Person.id'),
            ('comment_replyOf_post', 'Comment.id', 'Post.id'),
            ('person_isLocatedIn_place', 'Person.id', 'Place.id'),
            ('post_isLocatedIn_place', 'Post.id', 'Place.id'),
            ('forum_containerOf_post', 'Forum.id', 'Post.id')
        ]
        self.vertex_table = {}
        self.edge_table = {}
        self.t_graph = libmono.T_Graph("LDBC") # transformed graph
        
        total_start_time = time.time()
        
        load_start_time = time.time()
        self._load_data()
        load_end_time = time.time()
        print(f'Data loading time: {load_end_time - load_start_time:.2f} seconds', flush=True)
        
        edge_start_time = time.time()
        self._build_edge_table()
        edge_end_time = time.time()
        print(f'Edge table building time: {edge_end_time - edge_start_time:.2f} seconds', flush=True)
        
        index_start_time = time.time()
        self._build_index()
        index_end_time = time.time()
        print(f'Index building time: {index_end_time - index_start_time:.2f} seconds', flush=True)
        
        total_end_time = time.time()
        print(f'Total load time: {total_end_time - total_start_time:.2f} seconds', flush=True)

    
    def _load_data(self):
        for table_name in self.vertex_table_name:
            static_files = glob.glob(os.path.join(self.parent_path, "static", '*'))
            dynamic_files = glob.glob(os.path.join(self.parent_path, "dynamic", '*'))
            all_files = dynamic_files + static_files
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in all_files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                # print(f"Processing file: {file}")
                print(table_name, flush=True)
                table = VertexTable(file, table_name)
                self.vertex_table[table_name] = table
            
        
        # self.vertex_table["post"].df.loc[5239348, 'imageFile'] = 'photo1145141919810.jpg'
        # print(self.vertex_table["post"].df.loc[5239348, 'imageFile'])
        
        
    
    def _build_edge_table(self):
        for table_name, src_column, dst_column in self.edge_table_name:
            dynamic_files = glob.glob(os.path.join(self.parent_path, "dynamic", '*'))
            static_files = glob.glob(os.path.join(self.parent_path, "static", '*'))
            all_files = dynamic_files + static_files
            
            #files = glob.glob(os.path.join(self.parent_path, "dynamic", '*'))
            pattern = re.compile(f'^{table_name.lower()}(_\\d+_\\d+)?$', re.IGNORECASE)
            matched_files = [file for file in all_files if pattern.match(os.path.splitext(os.path.basename(file))[0].lower())]
            for file in matched_files:
                print(table_name, flush=True)
                if any(table_name == edge[0] for edge in self.edge_table_name):
                    edge_table = EdgeTable(file, table_name, src_column, dst_column, graph_type='homogeneous')
                else:
                    edge_table = EdgeTable(file, table_name, src_column, dst_column)
                self.edge_table[table_name] = edge_table
    
    def _build_index(self):
        self.vertex_table['organisation'].create_index('id')
        self.vertex_table['tagclass'].create_index('id')
        self.vertex_table['person'].create_index(['id'])
        self.vertex_table['comment'].create_index('id')
        self.vertex_table['forum'].create_index('id')
        self.vertex_table['post'].create_index('id')
        self.vertex_table['tag'].create_index('id')
        self.vertex_table['place'].create_index('id')
    

    def get_vertex_table(self, table_name):
        return self.vertex_table[table_name]
    
    def get_edge_table(self, table_name):
        return self.edge_table[table_name]
    
    # reorder the table by the order of the indices
    def reorder_table(self, table_name, indices):
        self.vertex_table[table_name].reorder_table(indices)
    
    def transform_graph(self):
        tot_start_time = time.time()
        # self.t_graph.transformVertexTable(self.vertex_table["post"])
        for table in self.vertex_table:
            print("Transforming VertexTable", table)
            start_time = time.time()
            self.t_graph.transformVertexTable(self.vertex_table[table])
            end_time = time.time()
            print(f"Transform {table} table finished in {end_time - start_time:.2f} seconds")

        for table in self.edge_table:
            print("Transforming EdgeTable", table)
            self.t_graph.transformEdgeTable(self.edge_table[table])
            print(f"Transform {table} table finished in {end_time - start_time:.2f} seconds")
            
        tot_end_time = time.time()
        print(f"Total transform finished in {tot_end_time - tot_start_time:.2f} seconds")
        return self.t_graph
    
    def save_graph(self):
        self.t_graph.saveGraph("/mnt/nvme/ldbc_dataset/sf10/ldbc_tgraph_edge.txt")
        self.t_graph.saveIdMap("/mnt/nvme/ldbc_dataset/sf10/ldbc_tgraph_idmap.pkl")

# path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvBasic-StringDateFormatter"
# path = "/mnt/nvme/ldbc_dataset/sf10/ori"
path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvBasic-StringDateFormatter"
ldbc = LDBC(path)
ldbc.transform_graph()
print("transform done", flush=True)
ldbc.save_graph()



