import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from loader.basic_table import BasicTable, EdgeTable


def reorder_heter(G: EdgeTable, target='out'):
    src_nodes = G.get_src_nodes()
    dst_nodes = G.get_dst_nodes()
    source_labels = {}
    target_labels = {}
    nodes = []
    if target == 'out':
        nodes = dst_nodes
        source_labels = {node: node for node in src_nodes}
        target_labels = {node: node for node in dst_nodes}
    else:
        nodes = src_nodes
        source_labels = {node: node for node in dst_nodes}
        target_labels = {node: node for node in src_nodes}
    for node in nodes:
        neighbors = G.get_neighbors(node)
        if not neighbors:
            continue
        label_count = {}
        for neighbor in neighbors:
            # print(neighbor)
            label = source_labels[neighbor]
            if label in label_count:
                label_count[label] += 1
            else:
                label_count[label] = 1

        max_label = max(label_count, key=label_count.get)
        target_labels[node] = max_label
    
    sorted_nodes = sorted(nodes, key=lambda node: target_labels[node])
    sorted_nodes = [int(node.split('_')[1]) for node in sorted_nodes]
    return sorted_nodes

    

path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_likes_comment_0_0.csv"
edge_table = EdgeTable(path, 'Person.id', 'Comment.id')
sorted_comment = reorder_heter(edge_table)
path = "/mnt/nvme/ldbc_dataset/social_network-sf10-CsvCompositeMergeForeign-LongDateFormatter/dynamic/comment_0_0.csv"
comment_table = BasicTable(path)
comment_table.create_index('id')
comment_table.reorder_table(sorted_comment)
path = "../inter_result/comment_0_0.csv"
comment_table.save(path)

