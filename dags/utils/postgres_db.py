import psycopg2

class PostgreSQLConnection:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("Conexión exitosa a PostgreSQL")
        except (Exception, psycopg2.Error) as error:
            print("Error al conectarse a PostgreSQL:", error)

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Desconexión exitosa de PostgreSQL")

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Error al ejecutar la consulta:", error)
            return None

    def insert_query(self, table, columns, values):
        try:
            # Construir la consulta SQL dinámicamente
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES "
            
            # Construir la parte de los valores de la consulta SQL
            query += ', '.join(['(' + ', '.join(['%s'] * len(row)) + ')' for row in values])

            # Combinar todos los valores en una sola lista
            flat_values = [val for sublist in values for val in sublist]
            
            # Ejecutar la consulta
            self.cursor.execute(query, flat_values)
            self.connection.commit()
            return None
        except (Exception, psycopg2.Error) as error:
            print("Error al ejecutar la consulta de insert:", error)
            return None
