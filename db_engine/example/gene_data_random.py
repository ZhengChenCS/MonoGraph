import numpy as np
import pandas as pd
import kuzu
import random
import string

def generate_random_string(length):
    """生成一个随机字符串，长度为 length"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def create_table(name, num_rows):
    """创建一个包含随机数据的表"""
    data = {
        'id': range(1, num_rows + 1),
        'content': [generate_random_string(random.randint(5, 20)) for _ in range(num_rows)]
    }
    df = pd.DataFrame(data)
    df.to_csv(f'/mnt/nvme/ldbc_dataset/example/{name}.csv', index=False)
    return df

def create_edge_table(name, from_table, to_table, num_edges):
    """创建一个边表，表示两个表之间的联系"""
    from_ids = from_table['id'].tolist()
    to_ids = to_table['id'].tolist()
    
    data = {
        'from_id': random.choices(from_ids, k=num_edges),
        'to_id': random.choices(to_ids, k=num_edges)
    }
    df = pd.DataFrame(data)
    df.to_csv(f'/mnt/nvme/ldbc_dataset/example/{name}.csv', index=False)
    return df

# table_A = create_table('A', 100)
# table_B = create_table('B', 100000)
# table_C = create_table('C', 10000000)

# # 假设每个边表有 15 条边
# edges_A_B = create_edge_table('edges_A_B', table_A, table_B, 100000)
# edges_B_C = create_edge_table('edges_B_C', table_B, table_C, 10000000)


table_A = create_table('A_big', 1000)
table_B = create_table('B_big', 1000000)
table_C = create_table('C_big', 200000000)

# 生成连续的边表
edges_A_B = create_edge_table('edges_A_B_big', table_A, table_B, 1000000)
edges_B_C = create_edge_table('edges_B_C_big', table_B, table_C, 200000000)

# 打印生成的边表
print("Edges A -> B:")
print(edges_A_B)
print("\nEdges B -> C:")
print(edges_B_C)
