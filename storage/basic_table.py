import pandas as pd
import numpy as np
import networkx as nx
import os
import pickle


class BasicTable:
    def __init__(self, path, table_name):
        # self.df = pd.read_csv(path, sep='|', mangle_dupe_cols=True)
        self.df = pd.read_csv(path, sep='|')
        self.header = self.df.columns.tolist()
        self.indexed_columns = set()
        self.name = table_name
    

    def get_df(self):
        return self.df

    def get_column_names(self):
        return self.header
    
    def get_column_data(self, column_name):
        if column_name in self.df.index.names:
            return self.df.index.get_level_values(column_name).tolist()
        elif column_name in self.df.columns:
            return self.df[column_name].to_numpy()
        else:
            raise ValueError(f"Column {column_name} does not exist in the DataFrame.")

    def create_index(self, column_name):
        if isinstance(column_name, list):
            for col in column_name:
                if col not in self.df.columns:
                    raise ValueError(f"Column {col} does not exist in the DataFrame.")
            self.df.set_index(column_name, inplace=True)
            self.indexed_columns.update(column_name)
        elif isinstance(column_name, str):
            if column_name not in self.df.columns:
                raise ValueError(f"Column {column_name} does not exist in the DataFrame.")
            self.df.set_index(column_name, inplace=True)
            self.indexed_columns.add(column_name)
        else:
            raise TypeError("column_name must be a string or a list of strings.")
    
    def get_data_by_index(self, index, index_column, columns=None):
        if index_column not in self.indexed_columns:
            raise ValueError(f"Column {index_column} is not indexed. Please index it before accessing.")
        
        # result = self.df[self.df[index_column] == index]
        # result = self.df.xs(index, level=index_column)
        if isinstance(self.df.index, pd.MultiIndex):
            result = self.df.xs(index, level=index_column)
        else:
            result = self.df.loc[index]
        if result.empty:
            raise ValueError(f"Index {index} does not exist in the DataFrame for column {index_column}.")
        
        if columns:
            if isinstance(columns, str):
                columns = [columns]
            index_columns = result.index.names
            output = {}
            for col in columns:
                if col in index_columns:
                    output[col] = result.index.get_level_values(col).tolist()
                else:
                    output[col] = result[col].tolist()
            return output
        return result

    def reorder_table(self, indices):
        if not isinstance(indices, list):
            raise TypeError("Indices must be a list.")
        current_index = set(self.df.index)

        if not set(indices).issubset(current_index):
            raise ValueError("Indices contain elements not in the DataFrame's index.")

        missing_indices = list(current_index - set(indices))

        full_indices = indices + missing_indices
        self.df = self.df.loc[full_indices].reset_index()
    
    def save(self, path):
        self.df.to_csv(path, sep='|', index=False)

    def __str__(self):
        return f"BasicTable with {len(self.df.columns)} columns: {self.df.columns.tolist()}"

    # 编码，避免table名，列名，主键，非主键hash为同一个值
    def table_encoding(self, key):
        return '#' + str(key)

    def column_encoding(self, key, table_id):
        return '$' + str(table_id) + '_' + str(key)

    def edge_id_encoding(self, key, table_id):
        return str(table_id) + "_" + str(key)

    def primary_key_encoding(self, key, table_id):
        return str(table_id) + "_" + str(key)