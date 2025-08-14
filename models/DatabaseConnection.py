from sqlalchemy import create_engine, URL


class DatabaseConnection:
    def __init__(self, db_usr, db_pwd, db_host, db_name):
        self.db_usr = db_usr
        self.db_pwd = db_pwd
        self.db_host = db_host
        self.db_name = db_name
        self.engine = None
        self.db_url = URL.create(
            "mssql+pyodbc",
            username=self.db_usr,
            password=self.db_pwd,
            host=self.db_host,
            database=self.db_name,
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "trustServerCertificate": "yes",
            },
        )

    def __del__(self):
        if self.engine:
            self.engine.dispose()

    def connect(self):
        try:
            self.engine = create_engine(self.db_url)
        except Exception as e:
            print(f"Error connecting to database: {e}")