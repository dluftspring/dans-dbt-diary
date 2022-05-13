import os
import sys
import snowflake.connector
from colorama import Fore
import click

def create_connection() -> snowflake.connector.connection:

    """
    Create connection to snowflake
    """

    con = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USERNAME'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

    return con


@click.command()
@click.option('--query', '-q', type=click.STRING, help='Query to execute', required=True)
def main(query: str) -> None:

    """
    Run a snowflake query from the command line. Make sure your query is wrapped in double quotes

    Example usage\n
    -------------\n
    >>> python run_snowflake_query.py -q "select * from mytable limit 10"\n
    >>> OK: Query executed successfully
    """

    con = create_connection()
    with con.cursor() as cur:
        cur.execute(query.lower().strip())
        res = cur.fetchall()

    if res:
        print(Fore.GREEN+"OK:"+Fore.WHITE+"Query executed successfully")
        sys.exit(0)
    
    print(Fore.RED+"FAILED:"+Fore.WHITE+"Query failed \n"+Fore.YELLOW+"{}".format(query))
    sys.exit(1)
