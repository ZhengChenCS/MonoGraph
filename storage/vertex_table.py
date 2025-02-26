import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(monograph_dir)

from storage.basic_table import BasicTable
from storage.transform_graph import transformed_graph

class VertexTable(BasicTable):
    def __init__(self, path, table_name):
        super().__init__(path, table_name)
    
    def transform(self, t_graph):
        """
        transform vertex table into transformed graph

        Args: 
            t_graph (transformed_graph): info of transformed graph
        """

        table_name = self.name
        col_names = self.get_column_names()

        t_graph.create_vertex(self.table_encoding(table_name))
        table_id = t_graph.get_id(self.table_encoding(table_name))

        # 创建每一个列
        for col_name in col_names:
            col_id = t_graph.create_vertex(self.column_encoding(col_name, table_id))
            t_graph.create_edge_mapping(table_id, col_id)
        
        primary_index = next(iter(self.indexed_columns))

        for cur in range(len(self.df)):
            primary_id = -1
            for col_name in col_names:
                col_id = t_graph.get_id(self.column_encoding(col_name, table_id))
                if col_name == primary_index:
                    data = self.df.index[cur]
                    primary_id = t_graph.create_vertex(self.primary_key_encoding(data, table_id))
                    t_graph.create_edge_mapping(primary_id, col_id)
                else:
                    data = self.df.iloc[cur][col_name]
                    value_id = t_graph.create_vertex(data)
                    t_graph.create_edge_mapping(value_id, col_id)
                    t_graph.create_edge_mapping(value_id, primary_id)
        
        # t_graph.print_vertex()
                    
        return t_graph

        
        
        



    def __str__(self):
        return f"VertexTable with {len(self.df.columns)} columns: {self.df.columns.tolist()}"