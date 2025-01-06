import kuzu

def profile(conn, query):
    response = conn.execute("PROFILE " + query)
    execution_time = response.get_execution_time()
    compiling_time = response.get_compiling_time()
    print(f"Execution time: {execution_time/1000:.4f}s, Compiling time: {compiling_time/1000:.4f}s")
    while response.has_next():
        sentence = response.get_next()
        for s in sentence:
            print(s)

def main(path):
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)
    place_ids = [1356, 1353, 519, 918, 211, 264, 512, 129, 280, 132]
    query = open("query.cypher", "r").read()
    for place_id in place_ids:
        query = query.replace("$placeId", str(place_id))
        profile(conn, query)
        # response = conn.execute(query)
        # while response.has_next():
        #     print(response.get_next())
        break

if __name__ == "__main__":
    db_path = "/mnt/nvme/KuzuDB/ldbc-10"
    main(db_path)