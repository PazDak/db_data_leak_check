"""
This is a class that searches a Database for matches to a regular expression. All Columns and Tables
"""
import re
from sqlalchemy import create_engine

class db_regex:
    """
    This class process the database for RegEx matches
    """
    connection_string = ""
    regex = ""
    def __init__(self,
                 hostname="localhost",
                 port="3306",
                 username="root",
                 password="",
                 database_name="test"):
        """
        Set the Parameters of the the connection to the MySQL Database
        :param hostname: The host name or IP address of the server
        :param port: the port number to connect to
        :param username: Username for access to the database
        :param password: Password for access to the database
        :param database_name: Database Name
        """
        self.connection_string = f'mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database_name}'
        self.engine = create_engine(self.connection_string)

    def set_regex(self, regular_expresion=""):
        """
        Sets the Regular Expression to use in the DB parser.
        :param regular_expresion:
        :return: None
        """
        self.regex = regular_expresion

    def search_all_tables(self) -> dict:
        """
        Execute this to run through all of the tables.
        :return: Returns a list of Database and Column Tuples that contain information matched by the RegEx
        """
        connection = self.engine.connect()
        all_tables = list()
        matches = list()
        table_results = connection.execute('show tables')
        for table_name_row in table_results:
            all_tables.append(table_name_row[0])

        for table in all_tables:
            results = connection.execute(f'describe {table}')
            rows = {}
            for row in results:
                rows[f'{row[0]}'] = {
                    'Field': row[0],
                    'Type': row[1],
                    'Null': row[2],
                    'Key': row[3],
                    'Default': row[4],
                    'extra': row[5]
                }
                if self.check_tuple(table=table, field=row[0]):
                    matches.append(f"{table}:{row[0]}")

        if matches.__len__() > 0:
            return matches
        return None

    def check_tuple(self, table, field) -> bool:
        """
        Function checks a specific tuple of a table and field with the stored RegEx statement
        :param table: Name of the table or schema in the database
        :param field: The field name of the table
        :return: True if it was found.
        """
        connection = self.engine.connect()
        command = f"select distinct {field} from {table}"
        distinct = connection.execute(command)
        for check_column in distinct:
            s = str(check_column[0])
            if s:
                if re.search(self.regex, s):
                    return True
        return False
