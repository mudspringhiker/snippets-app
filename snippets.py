import logging, argparse, psycopg2


# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)
# Connect to database from Python
logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect(database='snippets')
logging.debug("Database connection established.")


def put(name, snippet):
    """
    Store a snippet with an associated name.

    Returns the name and the snippet
    """
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    
    with connection, connection.cursor() as cursor:
        try:
            cursor.execute("insert into snippets values (%s, %s)", (name, snippet))
        except psycopg2.IntegrityError as e:
            connection.rollback()
            cursor.execute("update snippets set message=%s where keyword=%s", (snippet, name))
    
    logging.debug("Snippet stored successfully.")
    return name, snippet
    

def get(name):
    """
    Retrieve the snippet with a given name.

    If there is no such snippet, return '404: Snippet Not Found'.

    Returns the snippet.
    """
    logging.info("Retrieving the snippet for {!r}".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
    logging.debug("Snippet retrieved successfully.")
    
    if not row:
        # No snippet was found with that name
        return "404: Snippet Not Found"
    return row[0]
    
    
def catalog():
    """
    Retrieve names from the table order by name.
    """
    logging.info("Retrieving names order by name.")
    with connection, connection.cursor() as cursor:
        cursor.execute("select keyword from snippets order by keyword")
        rows = cursor.fetchall()
    logging.debug("Names of snippets retrieved successfully.")
    
    if not rows:
        # No names was found (table empty.)
        return "404: Table is empty"
    return rows
    
    
def search(string):
    """
    Retrieve snippets which contain a given string anywhere in
    
    the snippet. If there is no such snippet, return 'No snippet containing
    
    the string found'.
    """
    logging.info("Retrieving snippets containing the search string.")
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where message like %s", ('%'+string+'%',))
        rows = cursor.fetchall()
    logging.debug("Snippets containing the search string retrieved successfully.")
    
    if not rows:
        return "No snippets with the search string found."
    
    return rows
    
    
def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    put_parser.add_argument("snippet", help="Snippet text")
    
    # Subparser for the get command
    logging.debug("Constructing get subparser")
    put_parser = subparsers.add_parser("get", help="Retrieve a snippet")
    put_parser.add_argument("name", help="Name of the snippet")
    
    # Subparser for the catalog command
    logging.debug("Constructing catalog subparser")
    put_parser = subparsers.add_parser("catalog", help="Retrieve names of snippets")
    
    # Subparser for the search command
    logging.debug("Constructing search subparser")
    put_parser = subparsers.add_parser("search", help="Retrieve snippets containing search string")
    put_parser.add_argument("string", help="String  to search in snippet")
    
    arguments = parser.parse_args()

    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)
    command = arguments.pop("command")
    
    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command == "catalog":
        names = catalog()
        print("Retrieved names of snippets: {!r}".format(names))
    elif command == "search":
        snippets = search(**arguments)
        print("Snippets containing the string: {!r}".format(snippets))
        

if __name__ == "__main__":
    main()
