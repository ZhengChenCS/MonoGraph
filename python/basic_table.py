import pandas as pd
import numpy as np

class BasicTable:
    def __init__(self, path):
        self.df = pd.read_csv(path, sep='|')
        self.header = self.df.columns.tolist()

    def get_df(self):
        return self.df
    
    def get_column_names(self):
        return self.header
    
    def get_column_data(self, column_name):
        return self.df[column_name].tolist()
    
    def __str__(self):
        return f"BasicTable with {len(self.df.columns)} columns: {self.df.columns.tolist()}"

# path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter/static/organisation_0_0.csv"
# table = BasicTable(path)
# print(table)
# print(table.get_column_names())
# print(table.get_column_data('id'))
