import kuzu


def create_scheme(conn, scheme_cypher):
    conn.execute(scheme_cypher)

def copy_data(conn, copy_cypher):
    conn.execute(copy_cypher)

def init_db(db_path):
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)
    scheme_cypher = f"CREATE NODE TABLE A(ID INT64, content STRING, PRIMARY KEY(ID)); \
        CREATE NODE TABLE B(ID INT64, content STRING, PRIMARY KEY(ID)); \
        CREATE NODE TABLE C(ID INT64, content STRING, PRIMARY KEY(ID)); \
        CREATE REL TABLE A_B(FROM A TO B, MANY_MANY); \
        CREATE REL TABLE B_C(FROM B TO C, MANY_MANY);"
    create_scheme(conn, scheme_cypher)
    path = "/mnt/nvme/ldbc_dataset/example/"
    copy_cypher = f"COPY A FROM '{path}/A_big.csv' ; \
        COPY B FROM '{path}/B_big.csv' ; \
        COPY C FROM '{path}/C_big.csv' ; \
        COPY A_B FROM '{path}/edges_A_B_big.csv' ; \
        COPY B_C FROM '{path}/edges_B_C_big.csv' ;"
    copy_data(conn, copy_cypher)

def profile(conn, query):
    response = conn.execute("PROFILE " + query)
    execution_time = response.get_execution_time()
    compiling_time = response.get_compiling_time()
    print(f"Execution time: {execution_time/1000:.4f}s, Compiling time: {compiling_time/1000:.4f}s")
    while response.has_next():
        sentence = response.get_next()
        for s in sentence:
            print(s)

def run(conn, query):
    response = conn.execute(query)
    while response.has_next():
        sentence = response.get_next()
        for s in sentence:
            print(s)


def main(db_path):
    # init_db(db_path)
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)
    
    cypher = "MATCH (a:A {id: 93})-[:A_B]->(b:B)-[:B_C]->(c:C) RETURN a.id, b.id, c.id"
    cypher = "MATCH (c:C)<-[:B_C]-(:B) RETURN COUNT(DISTINCT c) AS countOfCWithNeighbors"
    # cypher = "MATCH (c:C) where c.id < 100 return count(c)"
    # cypher = "MATCH (c:C) return count(c)"
    run(conn, cypher)
    # profile(conn, cypher)
if __name__ == "__main__":
    db_path = "/mnt/nvme/KuzuDB/example_random_big"
    main(db_path)
