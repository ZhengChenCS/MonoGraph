import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(monograph_dir)
from storage.basic_table import BasicTable
from storage.edge_table import EdgeTable
from storage.vertex_table import VertexTable

class MonoGraph:
    def __init__(self):
        self.edge_tables = {}
    
    def load_edge_table(self, table_name, table):
        self.edge_tables[table_name] = table