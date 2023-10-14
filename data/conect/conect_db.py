import sqlalchemy as db
from sqlalchemy import create_engine
import os


def credentialGet():
        # Asignación de variables para la conexion de la base SQL
        user = os.environ.get("user_server")
        if user in os.environ:
            user = os.environ["user_server"]

        password = os.environ.get("password-server")
        if password in os.environ:
            password = os.environ["password-server"]

        server = "SYNBOG-DESK-148"
        database = "Estrategia"

        return user, password, server, database


def connectSQL():
    user, password, server, database = credentialGet()

    engine = create_engine(
        f"mssql+pyodbc://{server}/{database}?UID={user};PWD={password}&driver=ODBC+Driver+17+for+SQL+Server"
    )
    connection = engine.connect()

    return engine, connection
