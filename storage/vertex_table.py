import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(monograph_dir)

from storage.basic_table import BasicTable
from storage.transform_graph import transformed_graph

class VertexTable(BasicTable):
    def __init__(self, path):
        super().__init__(path)
    
    def transform(self, t_graph):
        """
        transform vertex table into transformed graph

        Args: 
            t_graph (transformed_graph): info of transformed graph
        """
        cols = self.df.columns.tolist()
        col_name = self.header
        table_name = self.name
        # (table_name, table_content),  = table.items()

        t_graph.create_vertex(self.table_encoding(table_name))
        table_id = t_graph.get_id(self.table_encoding(table_name))

        # col_name = table_content.col_name
        # cols = table_content.col

        primary_index = col_name.index('id')
        
        for i in range(len(col_name)):
            t_graph.create_vertex(self.column_encoding(col_name[i], table_id))
            col_id = t_graph.get_id(self.column_encoding(col_name[i], table_id))
            t_graph.create_edge_mapping(table_id, col_id)
            for value in cols[i]:
                if i == primary_index:
                    t_graph.create_vertex(self.primary_key_encoding(value, table_id))
                    value_id = t_graph.get_id(self.primary_key_encoding(value, table_id))
                    t_graph.create_edge_mapping(col_id, value_id)
                else:
                    t_graph.create_vertex(value)
                    value_id = t_graph.get_id(value)
                    t_graph.create_edge_mapping(col_id, value_id)
        
        

        # create edge from primary key to other value
        for i in range(len(cols[i])):
            for j in range(len(col_name)):
                if j == primary_index:
                    continue
                else:
                    t_graph.create_edge(cols[j][i], cols[j][primary_index])

        return t_graph