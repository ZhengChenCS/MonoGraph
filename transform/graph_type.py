import os
import sys
import numpy as np
import pickle

class graph_t:
    def __init__(self, name):
        self.vertex = []
        self.edge = []
        self.max_id = 0
        self.id_map = {}
        self.name = name
        self.vertex_cnt = 0
        self.edge_cnt = 0
    

    def create_vertex(self, key):
        if key in self.id_map:
            return
        else:
            self.id_map[key] = self.max_id
            self.max_id += 1
            self.vertex_cnt +=1
    
    def get_id(self, key):
        if key not in self.id_map:
            print(f'{key} not exist in id map.')
            exit(0)
        else:
            return self.id_map[key]

    def create_edge(self, src, dst):
        self.edge.append((self.get_id(src), self.get_id(dst)))
        self.edge_cnt += 1
    
    def create_edge_mapping(self, src, dst):
        self.edge.append((src, dst))
        self.edge_cnt += 1
    
    def __repr__(self):
        return f"vertex_cnt:{self.vertex_cnt}, edge_cnt:{self.edge_cnt}"
    
    def print_vertex(self):
        print(self.id_map)
    

    def save_graph(self, path):
        # np_graph = np.array(self.edge)
        # np.save(path, self.edge)
        # print(f"graph has saved into {path}")

        output_str = '\n'.join([f'{pair[0]} {pair[1]}' for pair in self.edge])
        with open(path, 'w') as file:
            file.write(output_str)

    def save_idmap(self, path):
        with open(path, 'wb') as file:
            pickle.dump(self.id_map, file)
        print(f'id map has saved into {path}')
    

    
