import sqlparse


class SQLEngine:
    """
        This is a helper class used to parse SQL commands
    """

    # name of the table
    table_name = None

    # table fields, e.g varchar, integer etc
    class_fields = {}

    # primary key used in the table
    primary_key = None

    def __parse_class_fields(self, tokens):
        """
        Parse the CREATE TABLE (...attributes) statement
        :param tokens: parsed SQL tokens in the CREATE table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        tokens = list(filter(lambda token: not token.is_whitespace, tokens))
        field = None
        name = None
        i = 0
        while i < len(tokens):
            token = tokens[i]
            i = i + 1
            if not token.normalized == '(':
                if token.is_keyword:
                    if token.normalized == 'PRIMARY':
                        self.primary_key = name
                        self.class_fields[name] = field
                        next_token = tokens[i]
                        index = next_token.value.rfind(',')
                        if index == -1:
                            name = None
                        else:
                            name = next_token.value[index + 1:]
                            i = i + 1
                    elif token.normalized == 'KEY':
                        continue
                    else:
                        raise ValueError("Unsupported keyword: %s" % token.value)
                elif token.ttype is None or token.ttype == sqlparse.tokens.Keyword:
                    name = token.value
                elif token.ttype == sqlparse.tokens.Name.Builtin:
                    if token.normalized == 'float' or token.normalized == 'varchar' or token.normalized == 'integer':
                        field = token.normalized
                    else:
                        raise ValueError("Unsupported data type: " + token.value)
                elif token.normalized == ')' or token.normalized == ',':
                    if name is not None and field is not None:
                        self.class_fields[name] = field
                    field = None
                    name = None
                else:
                    raise ValueError("There is an unsupported class field")
        return {
            "type": "CREATE TABLE",
            "table_name": self.table_name,
            "attributes": self.class_fields,
            "primary_key": self.primary_key
        }

    def __parse_create_table_statement(self, tokens):
        """
        Ensure that the CREATE TABLE statement is formatted correctly
        :param tokens: parsed SQL tokens in the CREATE table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        for token in tokens:
            if not token.is_whitespace:
                if isinstance(token, sqlparse.sql.Identifier):
                    self.table_name = token.value
                elif isinstance(token, sqlparse.sql.Parenthesis):
                    return self.__parse_class_fields(token.tokens)
                elif token.is_group:
                    group_tokens = token.tokens
                    for j in range(0, len(group_tokens)):
                        group_token = group_tokens[j]
                        if isinstance(group_token, sqlparse.sql.Identifier):
                            self.table_name = group_token.value
                        elif isinstance(group_token, sqlparse.sql.Parenthesis):
                            return self.__parse_class_fields(group_token.tokens)
                        else:
                            raise ValueError("Expecting identifier after CREATE TABLE or parenthesis after identifier")
                else:
                    raise ValueError("Expecting grouped statement after CREATE table")
        raise ValueError("Empty CREATE TABLE statement")

    def __parse_create_statement(self, tokens):
        """
        Ensure that the CREATE statement is formatted correctly
        :param tokens: parsed SQL tokens in the CREATE table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        for i in range(0, len(tokens)):
            token = tokens[i]
            if not token.is_whitespace:
                if token.normalized == 'TABLE':
                    return self.__parse_create_table_statement(tokens[i + 1:])
                else:
                    raise ValueError("Unsupported operation: CREATE " % token.normalized)
        raise ValueError("Empty CREATE statement")

    @staticmethod
    def __parse_where_conditions(tokens):
        """
        Parse the WHERE conditions, e.g WHERE table_column = value
        :param tokens: parsed SQL tokens in the CREATE table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        conditions = {}
        for i in range(0, len(tokens)):
            token = tokens[i]
            if isinstance(token, sqlparse.sql.Comparison):
                conditions[token.left.value] = token.right.value.strip("\"").strip("'")
                return conditions
        raise ValueError("Invalid comparison in WHERE statement")

    def __parse_select_statement(self, tokens):
        """
        Ensure that the SELECT statement is formatted correctly
        :param tokens: parsed SQL tokens in the SELECT table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        conditions = {}
        for i in range(0, len(tokens)):
            token = tokens[i]
            if not token.is_whitespace:
                if isinstance(token, sqlparse.sql.Identifier):
                    self.table_name = token.value
                elif isinstance(token, sqlparse.sql.Where):
                    conditions = self.__parse_where_conditions(token.tokens[1:])
                elif token.ttype == sqlparse.tokens.Punctuation:
                    continue
                else:
                    raise ValueError("Unhandled operation in SELECT statement")
        if self.table_name is None:
            raise ValueError("Missing identifier in SELECT statement")
        else:
            return {
                "type": "SELECT",
                "table_name": self.table_name,
                "conditions": conditions
            }

    def __process_into_values(self, tokens):
        """
        Ensure that the INSERT INTO statement is formatted correctly
        :param tokens: parsed SQL tokens in the INSERT INTO table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        values = []
        for i in range(0, len(tokens)):
            token = tokens[i]
            if not token.is_whitespace:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for valueToken in token.tokens:
                        if not valueToken.is_whitespace:
                            def is_int(number):
                                """
                                Check if a variable is an integer
                                :param number: variable to check
                                :return: true if the variable is an integer, false otherwise
                                """
                                try:
                                    int(number)
                                    return True
                                except:
                                    return False

                            def is_float(number):
                                """
                                Check if a variable is a float
                                :param number: variable to check
                                :return: true if the variable is an float, false otherwise
                                """
                                try:
                                    float(number)
                                    return True
                                except:
                                    return False

                            value = valueToken.value
                            if is_int(value):
                                values.append(int(value))
                            elif is_float(value):
                                values.append(float(value))
                            elif valueToken.ttype is None:
                                value = value.strip("\"").strip("'")
                                values.append(value)
        if len(values) == 0:
            raise ValueError("No values provided")
        else:
            return {
                "type": "INSERT INTO",
                "table_name": self.table_name,
                "values": values
            }

    def __parse_insert_statement(self, tokens):
        """
        Ensure that the INSERT statement is formatted correctly
        :param tokens: parsed SQL tokens in the INSERT table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        found_into = False
        for i in range(0, len(tokens)):
            token = tokens[i]
            found_into = found_into or (
                    token.normalized == 'INTO' and token.ttype == sqlparse.tokens.Keyword and token.is_keyword)
            if not token.is_whitespace:
                if isinstance(token, sqlparse.sql.Identifier):
                    self.table_name = token.value
                elif isinstance(token, sqlparse.sql.Values):
                    values = token.tokens[1:]
                    if len(values) == 0: raise ValueError("Missing VALUES parameter")
                    values = values[0]
                    if isinstance(values, sqlparse.sql.Parenthesis):
                        return self.__process_into_values(values.tokens)
                    else:
                        raise ValueError("Expecting parenthesis in VALUES")
        if found_into:
            raise ValueError("Missing VALUES parameters")
        else:
            raise ValueError("Missing INTO keyword")

    def __parse_truncate_statement(self, tokens):
        """
        Ensure that the TRUNCATE statement is formatted correctly.
        This statement will delete all the rows in a table
        :param tokens: parsed SQL tokens in the TRUNCATE table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        conditions = {}
        for i in range(0, len(tokens)):
            token = tokens[i]
            if isinstance(token, sqlparse.sql.Identifier):
                self.table_name = token.value
            elif isinstance(token, sqlparse.sql.Where):
                conditions = self.__parse_where_conditions(token.tokens[1:])
        if self.table_name is None:
            raise ValueError("Missing identifier for TRUNCATE statement")
        else:
            return {
                "type": "TRUNCATE",
                "table_name": self.table_name,
                "conditions": conditions
            }

    def __parse_drop_statement(self, tokens):
        """
        Ensure that the DROP statement is formatted correctly.
        :param tokens: parsed SQL tokens in the DROP table statement
        :return: a dictionary containing the information in the parsed SQL statement
        :raises ValueError if the provided SQL statement is invalid
        """
        found_table = False
        for token in tokens:
            found_table = found_table or (
                    token.normalized == 'TABLE' and token.ttype == sqlparse.tokens.Keyword and token.is_keyword)
            if isinstance(token, sqlparse.sql.Identifier):
                self.table_name = token.value
        if found_table:
            if self.table_name is None:
                raise ValueError("Missing identifier in DROP statement")
            else:
                return {
                    "type": "DROP TABLE",
                    "table_name": self.table_name
                }
        else:
            raise ValueError("Missing TABLE keyword in DROP table")

    @staticmethod
    def __alias_sql(sql):
        """
        Allow aliases for SQL commands for shortcuts.
        The current mappings allowed are:
        SWIPE -> SELECT
        HATCH -> CREATE TABLE
        CHOMP -> TRUNCATE
        SWAMP -> DROP TABLE
        :param sql: SQL statement in String format
        :return: the SQL statement with the aliases replaced with the SQL-compliant keywords
        """
        sql_upper = sql.upper().strip()
        if sql_upper.startswith('SWIPE'):
            sql = "SELECT" + sql[len("SWIPE"):]
        elif sql_upper.startswith('HATCH'):
            sql = "CREATE TABLE" + sql[len("HATCH"):]
        elif sql_upper.startswith('CHOMP'):
            sql = "TRUNCATE" + sql[len("CHOMP"):]
        elif sql_upper.startswith('SWAMP'):
            sql = "DROP TABLE" + sql[len("SWAMP"):]
        return sql

    def parse_sql(self, sql):
        """
        Parse an SQL statement
        :param sql: SQL statement to parse
        :return: a dictionary containing the features of the SQL statement
        :raises: ValueError if the SQL statement contains invalid fields
        """
        self.table_name = None
        sql = self.__alias_sql(sql)
        parsed = sqlparse.parse(sql)
        if len(parsed) > 0:
            tokens = parsed[0].tokens
            for tokenId in range(0, len(tokens)):
                token = tokens[tokenId]
                if not token.is_whitespace:
                    remaining_tokens = tokens[tokenId + 1:]
                    if token.normalized == 'CREATE':
                        return self.__parse_create_statement(remaining_tokens)
                    elif token.normalized == 'SELECT':
                        return self.__parse_select_statement(remaining_tokens)
                    elif token.normalized == 'INSERT':
                        return self.__parse_insert_statement(remaining_tokens)
                    elif token.normalized == 'TRUNCATE':
                        return self.__parse_truncate_statement(remaining_tokens)
                    elif token.normalized == 'DROP':
                        return self.__parse_drop_statement(remaining_tokens)
                    else:
                        raise ValueError("Unsupported operation: " + token.normalized)
        raise ValueError("Empty or invalid SQL statement")
