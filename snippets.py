import logging
import argparse
import psycopg2


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
    cursor = connection.cursor()
    command = "insert into snippets values (%s, %s)"
    cursor.execute(command, (name, snippet))
    connection.commit()
    logging.debug("Snippet stored successfully.")
    # logging.error("FIXME: Unimplemented - put({!r}, {!r})".format(name, snippet))
    return name, snippet
    

def get(name):
    """Retrieve the snippet with a given name.

    If there is no such snippet, return '404: Snippet Not Found'.

    Returns the snippet.
    """
    logging.info("Retrieving the snippet for {!r}".format(name))
    cursor = connection.cursor()
    command = "select * from snippets where keyword=(%s)"
    cursor.execute(command, (name,))
    snippet = cursor.fetchone()
    connection.commit()
    logging.debug("Snippet retrieved successfully.")
    if not snippet:
        # No snippet was found with that name
        return "404: Snippet Not Found"
    #logging.error("FIXME: Unimplemented - get({!r})".format(name))
    return snippet[1]
    
    
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


if __name__ == "__main__":
    main()
