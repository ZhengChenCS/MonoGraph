import os 
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
monograph_dir = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(monograph_dir)

from storage.basic_table import BasicTable

class VertexTable(BasicTable):
    def __init__(self, path):
        super().__init__(path)