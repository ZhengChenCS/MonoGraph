import kuzu

scheme_cypher = open("schema.cypher", "r").read()
copy_cypher = open("copy.cypher", "r").read()

def create_scheme(conn, scheme_cypher):
    conn.execute(scheme_cypher)

def copy_data(conn, copy_cypher, prefix):
    updated_cypher = copy_cypher.replace('dataset', prefix)
    conn.execute(updated_cypher)

def main():
    db = kuzu.Database("/mnt/nvme/KuzuDB/exmaple1_ori")
    conn = kuzu.Connection(db)
    create_scheme(conn, scheme_cypher)
    prefix = "/mnt/nvme/ldbc_dataset/example1/ori"
    copy_data(conn, copy_cypher, prefix)
    conn.close()

if __name__ == "__main__":
    main()