import kuzu

def main() -> None:
    db_path = "/mnt/nvme/KuzuDB/example1.db"
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)


main()