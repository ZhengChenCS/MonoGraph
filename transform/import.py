import os
import sys
from table_type import table_t
from graph_type import graph_t
from key_type import table_encoding, column_encoding, edge_id_encoding, primary_key_encoding



def read_entire_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    return [line.strip() for line in lines]

def read_table(dir_path):
    col_name = read_entire_file(dir_path+"/col.name")
    col_name = col_name[0]
    col_name = col_name.split('\n')[0]
    col_name = col_name.split('|')
    content = []
    for i in range(len(col_name)):
        col_content = read_entire_file(dir_path+"/"+str(i)+".col")
        content.append(col_content)
    return table_t(col_name, content)



'''
load vertex table
create node: 
        1. table_name
        2. col_name
        3. value
create edge:
        1. table_name -- column_name
        2. value -- column_name
        3. primary_key -- other prop
'''

def import_vertex_table(graph, table):
    (table_name, table_content),  = table.items()
    graph.create_vertex(table_encoding(table_name))
    table_id = graph.get_id(table_encoding(table_name))
    col_name = table_content.col_name
    cols = table_content.col

    primary_index = col_name.index('id')
    for i in range(len(col_name)):
        graph.create_vertex(column_encoding(col_name[i], table_id))
        col_id = graph.get_id(column_encoding(col_name[i], table_id))
        graph.create_edge_mapping(table_id, col_id)
        for value in cols[i]:
            if i == primary_index:
                graph.create_vertex(primary_key_encoding(value, table_id))
                value_id = graph.get_id(primary_key_encoding(value, table_id))
                graph.create_edge_mapping(col_id, value_id)
            else:
                graph.create_vertex(value)
                value_id = graph.get_id(value)
                graph.create_edge_mapping(col_id, value_id)
    
    

    # create edge from primary key to other value
    for i in range(len(cols[i])):
        for j in range(len(col_name)):
            if j == primary_index:
                continue
            else:
                graph.create_edge(cols[j][i], cols[j][primary_index])
                

'''
load edge table
create node: 
    1. table_name(edge_label)
    2. edge_id(create)
    3. edge_prop
    4. edge_prop_value
create edge: 
    1. edge_label -- edge_id
    2. edge_id -- prop_value
    3. prop_name - prop_value
    4. src_id - edge_id - dst_id
'''

def import_edge_table(graph, table):
    (table_name, table_content), = table.items()
    col_name = table_content.col_name
    cols = table_content.col


    graph.create_vertex(table_encoding(table_name))
    table_id = graph.get_id(table_encoding(table_name))

    num_rows = len(cols[0])
    num_cols = len(col_name)

    
    if table_name == "Person_studyAt_University":
        '''
        {'Person_studyAt_University': ['creationDate', 'PersonId', 'UniversityId', 'classYear']}
        '''
        src_index = 1 # PersonId
        dst_index = 2 # UniversityId
        src_table_name = "Person"
        dst_table_name = "Organisation"
    elif table_name == "Comment_hasTag_Tag":
        '''
        {'Comment_hasTag_Tag': ['creationDate', 'CommentId', 'TagId']}
        '''
        src_index = 1 # CommentId
        dst_index = 2 # TagId
        src_table_name = "Comment"
        dst_table_name = "Tag"
    elif table_name == "Person_hasInterest_Tag":
        '''
        {'Person_hasInterest_Tag': ['creationDate', 'PersonId', 'TagId']}
        '''
        src_index = 1
        dst_index = 2
        src_table_name = "Person"
        dst_table_name = "Tag"
    elif table_name == "Person_workAt_Company":
        '''
        {'Person_workAt_Company': ['creationDate', 'PersonId', 'CompanyId', 'workFrom']}
        '''
        src_index = 1
        dst_index = 2
        src_table_name = "Person"
        dst_table_name = "Organisation"
    elif table_name == "Person_knows_Person":
        '''
        {'Person_knows_Person': ['creationDate', 'Person1Id', 'Person2Id']}
        '''
        src_index = 1
        dst_index = 2
        src_table_name = "Person"
        dst_table_name = "Person"
    elif table_name == "Forum_hasMember_Person":
        '''
        {'Forum_hasMember_Person': ['creationDate', 'ForumId', 'PersonId']}
        '''
        src_index = 1
        dst_index = 2
        src_table_name = "Forum"
        dst_table_name = "Person"
    elif table_name == "Person_likes_Comment":
        '''
        {'Person_likes_Comment': ['creationDate', 'PersonId', 'CommentId']}
        '''
        src_index = 1
        dst_index = 2
        src_table_name = "Person"
        dst_table_name = "Comment"
    elif table_name == "Post_hasTag_Tag":
        '''
        {'Post_hasTag_Tag': ['creationDate', 'PostId', 'TagId']}
        '''
        src_index = 1
        dst_index = 2
        src_table_name = "Post"
        dst_table_name = "Tag"
    elif table_name == "Forum_hasTag_Tag":
        '''
        {'Forum_hasTag_Tag': ['creationDate', 'ForumId', 'TagId']}
        '''
        src_index = 1
        dst_index = 2
        src_table_name = "Forum"
        dst_table_name = "Tag"
    elif table_name == "Person_likes_Post": # 边表需要告知src, dst的名称，此处为列举，到时候改写导入逻辑时需要优化
        '''
        {'Person_likes_Post': ['creationDate', 'PersonId', 'PostId']}
        '''
        src_index = 1
        dst_index = 2
        src_table_name = "Person"
        dst_table_name = "Post"
    else:
        print(f"{table_name} not exist.")
        exit(0)
    src_table_id = graph.get_id(table_encoding(src_table_name))
    dst_table_id = graph.get_id(table_encoding(dst_table_name))
    for i in range(num_cols):
        graph.create_vertex(column_encoding(col_name[i], table_id))
        col_name_id = graph.get_id(column_encoding(col_name[i], table_id))
        graph.create_edge_mapping(col_name_id, table_id)
    for i in range(num_rows):
        graph.create_vertex(edge_id_encoding(i, table_id))
        edge_id = graph.get_id(edge_id_encoding(i, table_id))
        graph.create_edge_mapping(edge_id, table_id)
    ## create edge prop value (not including src and dst)
    for i in range(num_cols):
        if i == src_index or i == dst_index:
            continue
        col_name_id = graph.get_id(column_encoding(col_name[i], table_id))
        for j in range(num_rows):
            graph.create_vertex(cols[i][j]) # create value
            value_id = graph.get_id(cols[i][j])
            graph.create_edge_mapping(col_name_id, value_id)
            graph.create_edge(edge_id_encoding(j, table_id), cols[i][j])
    ## create edge from src to dst
    for i in range(num_rows):
        edge_id = graph.get_id(edge_id_encoding(i, table_id))
        src_value = cols[src_index][i]
        dst_value = cols[dst_index][i]
        src_id = graph.get_id(primary_key_encoding(src_value, src_table_id))
        dst_id = graph.get_id(primary_key_encoding(dst_value, dst_table_id))
        graph.create_edge_mapping(edge_id, src_id)
        graph.create_edge_mapping(edge_id, dst_id)


            
        


if __name__ == '__main__':
    dataset_dir = "/Users/zhengchencs/Desktop/github/LDBC_dataset/sf-bi-1-csv"
    static_path = dataset_dir + "/static"
    dynamic_path = dataset_dir + "/dynamic"
    static_table_name = ['Organisation',  'Place', 'Tag', 'TagClass']
    dynamic_table_name = ['Comment', 'Person', 'Person_studyAt_University', 'Comment_hasTag_Tag', 
    'Person_hasInterest_Tag',  'Person_workAt_Company', 
    'Forum', 'Person_knows_Person', 'Post', 'Forum_hasMember_Person',  'Person_likes_Comment', 'Post_hasTag_Tag',
    'Forum_hasTag_Tag', 'Person_likes_Post']

    vertex_table_name = ['Comment', 'Person', 'Forum', 'Post']
    edge_table_name = ['Person_studyAt_University', 'Comment_hasTag_Tag', 
    'Person_hasInterest_Tag', 'Person_workAt_Company',
    'Person_knows_Person', 'Forum_hasMember_Person', 'Person_likes_Comment',
    'Post_hasTag_Tag', 'Forum_hasTag_Tag', 'Person_likes_Post'
    ]
    
    #当前是把读入的table作为外部变量，而不是存在graph类里头
    #static_table.items()和vertex_table_name是vertex表，edge_table_name是edge表
    static_table = {}
    for name in static_table_name:
        table = read_table(static_path+"/"+name)
        static_table[name] = table
    
    dynamic_table = {}
    for name in dynamic_table_name:
        table = read_table(dynamic_path+"/"+name)
        dynamic_table[name] = table

    vertex_table = []
    edge_table = []

    # print(static_table)
    for key, value in static_table.items():
        vertex_table.append({key:value})
    
    for name in vertex_table_name:
        vertex_table.append({name:dynamic_table[name]})
    for name in edge_table_name:
        edge_table.append({name:dynamic_table[name]})

    
    graph = graph_t("LDBC")


    '''
    import vertex table
    '''
    for table in vertex_table:
        print(f"import vertex table:{table}")
        import_vertex_table(graph, table)
        print(graph)

    print("Import vertex table finished.") 
    print(graph)
    
    '''
    import edge table
    '''
    
    for table in edge_table:
        print(f"import edge table:{table}")
        import_edge_table(graph, table) 
        print(graph)
    print("Import edge graph finished.")
    print(graph)


    # graph.save_graph("graph.npy")
    graph.save_graph("edge.txt")
    graph.save_idmap("idmap.pkl")

