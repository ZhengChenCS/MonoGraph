import numpy as np
import pandas as pd
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
    df.to_csv(f'/mnt/nvme/ldbc_dataset/example/{name}-seq.csv', index=False)
    return df

def create_continuous_edge_table(name, from_table, to_table, num_edges):
    """创建一个边表，使得每个节点的邻居是连续的"""
    from_ids = from_table['id'].tolist()
    to_ids = to_table['id'].tolist()
    
    edges = []

    while len(edges) < num_edges:
        # 随机选择一个 from_id
        from_id = random.choice(from_ids)
        # 随机选择一个起始点
        start_index = random.randint(0, len(to_ids) - 1)
        # 随机选择邻居的数量
        num_neighbors = random.randint(1, min(10, len(to_ids) - start_index))
        # 选择连续的 to_id 作为邻居
        neighbors = to_ids[start_index:start_index + num_neighbors]
        for to_id in neighbors:
            if len(edges) < num_edges:
                edges.append((from_id, to_id))
            else:
                break
    
    # 将边表转换为 DataFrame
    df = pd.DataFrame(edges, columns=['from_id', 'to_id'])
    
    # 保存为 CSV 文件
    df.to_csv(f'/mnt/nvme/ldbc_dataset/example/{name}-seq.csv', index=False)
    return df

table_A = create_table('A_big', 1000)
table_B = create_table('B_big', 1000000)
table_C = create_table('C_big', 200000000)

# 生成连续的边表
edges_A_B = create_continuous_edge_table('edges_A_B_big', table_A, table_B, 1000000)
edges_B_C = create_continuous_edge_table('edges_B_C_big', table_B, table_C, 200000000)

# 打印生成的边表
print("Continuous Edges A -> B:")
print(edges_A_B.head())
print("\nContinuous Edges B -> C:")
print(edges_B_C.head())
