import os
import sys


'''
col_name: []
col: string[]
'''

class table_t:
    def __init__(self, col_name, col):
        self.col_name = col_name
        self.col = col
        # self.col_type = col_type
    

    def __str__(self):
        return self.col_name
    
    def __repr__(self):
        return f"{self.col_name}"
    
    
    



    
    