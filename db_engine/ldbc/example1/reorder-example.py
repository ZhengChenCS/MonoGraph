import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from storage.basic_table import BasicTable
# from storage.edge_table import EdgeTable


class scheme:
    def __init__(self, root_path):
        self.root_path = root_path
        self.table = {}
        place = BasicTable(os.path.join(root_path, 'place_0_0.csv'))
        # person_isLocatedIn_place = EdgeTable(os.path.join(root_path, 'person_isLocatedIn_place_0_0.csv'))
        person_isLocatedIn_place = BasicTable(os.path.join(root_path, 'person_isLocatedIn_place_0_0.csv'))
        person = BasicTable(os.path.join(root_path, 'person_0_0.csv'))
        person_likes_comment = BasicTable(os.path.join(root_path, 'person_likes_comment_0_0.csv'))
        # person_likes_comment = EdgeTable(os.path.join(root_path, 'person_likes_comment_0_0.csv'))
        comment = BasicTable(os.path.join(root_path, 'comment_0_0.csv'))
        self.table['place'] = place
        self.table['person_isLocatedIn_place'] = person_isLocatedIn_place
        self.table['person'] = person
        self.table['person_likes_comment'] = person_likes_comment
        self.table['comment'] = comment
    
    def save(self, output_path):
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        self.table['place'].save(f'{output_path}/place_0_0.csv')
        self.table['person_isLocatedIn_place'].save(f'{output_path}/person_isLocatedIn_place_0_0.csv')
        self.table['person'].save(f'{output_path}/person_0_0.csv')
        self.table['person_likes_comment'].save(f'{output_path}/person_likes_comment_0_0.csv')  
        self.table['comment'].save(f'{output_path}/comment_0_0.csv')
    
    def reorder(self):
        label_id_map = {} #对于每行数据，建立place_id->index(每行索引)的映射
        place_table = self.table['place']
        for index, row in place_table.df.iterrows():
            label = f"label_{index}"
            place_id = row['id']
            label_id_map[place_id] = labelxiaow
        person_isLocatedIn_place_table = self.table['person_isLocatedIn_place']
        for index, row in person_isLocatedIn_place_table.df.iterrows():
            person_id = row['from']
            place_id = row['to']
            person_isLocatedIn_place_table.df.at[index, 'from'] = label_id_map[person_id]
            person_isLocatedIn_place_table.df.at[index, 'to'] = label_id_map[place_id]
        return label_id_map

root_path = '/mnt/nvme/ldbc_dataset/csv'
scheme = scheme(root_path)
scheme.save('/mnt/nvme/ldbc_dataset/example1_reorder')

