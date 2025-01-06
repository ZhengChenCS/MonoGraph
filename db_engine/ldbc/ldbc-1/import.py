import kuzu

scheme_cypher = open("../schema.cypher", "r").read()
copy_cypher = open("../copy.cypher", "r").read()

def create_scheme(conn, scheme_cypher):
    conn.execute(scheme_cypher)

def copy_data(conn, copy_cypher, prefix):
    updated_cypher = copy_cypher.replace('dataset/ldbc-1/csv', prefix)
    conn.execute(updated_cypher)



# conn = kuzu.Connection(db_path="/mnt/nvme/ldbc_dataset/ldbc")
# conn.query(copy_cypher)
# conn.close()
def main():
    db = kuzu.Database("/mnt/nvme/KuzuDB/ldbc-1")
    conn = kuzu.Connection(db)
    create_scheme(conn, scheme_cypher)
    prefix = "/mnt/nvme/ldbc_dataset/csv"
    copy_data(conn, copy_cypher, prefix)
    conn.close()

if __name__ == "__main__":
    main()
