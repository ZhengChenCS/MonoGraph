import kuzu

def main(db_path):
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)
    query = open("../queries/bi-1.cypher", "r").read()
    query = query.replace("$datetime", "datetime('2011-12-01T00:00:00.000')")
    response = conn.execute(query)
    while response.has_next():
        print(response.get_next())
    
db_path = "/mnt/nvme/KuzuDB/ldbc-1"
main(db_path)