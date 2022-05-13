import os
import sys
import logging
import re
import argparse
from pathlib import Path
from dotenv import load_dotenv; load_dotenv('.env')
import snowflake.connector
from os import PathLike
from typing import List

#@TODO - abstract calls to 'models' and 'docs' to check dbt_project.yml automatically
#Should work since the script is always run from the root project directory

class ModelExistsError(Exception):

    def __init__(self, model):
        self.model=model
        self.message=f'Model {self.model} schema file already exists and can\'t be overwritten'
        super().__init__(self.message)

    def __str__(self):
        return f'{self.message}'

class NotFoundError(Exception):

    """
    General error class for not finding a file/directory or table name
    """
    
    def __init__(self, name):
        self.name=name
        self.message=f'Couldn\'t find {self.name} please check your typing and sub directories'

    def __str__(self):
        return f'{self.message}'

def create_connection() -> snowflake.connector.connection:

    """
    Create connection to snowflake
    """

    con = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PWD'),
        passcode=os.getenv('SNOWFLAKE_PASSCODE'), #Avoids MFA pushes
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WH'),
        database=os.getenv('SNOWFLAKE_DB'),
        schema='INFORMATION_SCHEMA'
    )

    return con

def get_models_from_directory(directory: PathLike) -> List[str]:

    """
    Returns all models from a specified directory
    """
    
    full_path=list(Path('models').rglob(directory))
    assert len(full_path)==1, f"Multiple matches for {directory} try supplying the full sub directory"
    sql_regex=re.compile('\.sql')
    model_list=list(filter(sql_regex.search, os.listdir(full_path[0])))

    return list(map(lambda x: x.replace('.sql',''), model_list))

def find_model(table: str) -> PathLike:

    """
    Traverses project directory and finds the relative path of model
    """

    path_list = []
    for path in Path('models').rglob(table+'.*'):
        path_list.append(path)

    if not path_list:
        raise NotFoundError(table)

    target_model=path_list[0]

    if os.path.exists(str(target_model).replace('.sql','.yml')):
        raise ModelExistsError(table)
    
    return target_model

def doc_lkp(col: str) -> bool:
    
    """
    Looks for existing doc blocks in docs/definitions.md
    """

    if not os.path.exists('docs/definitions.md'):
        raise NotFoundError('docs/definitions.md')

    with open('docs/definitions.md') as f:
        doc=f.read() #returns a string

    doc_strings=re.findall('(?<={% docs)(.*)(?=%})',doc)
    clean_doc_strings=list(map(str.strip,doc_strings))

    return col in clean_doc_strings


def make_schema(tables: str or List[str], directory: PathLike = None) -> None:

    """
    Generates a table.yml file based on results of querying the information schema
    """
    
    #validation conditions
    if not isinstance(tables,List):
        tables=[tables]

    if directory is not None:
        if tables[0] is None:
            tables=get_models_from_directory(directory)
        else:
            tables.extend(get_models_from_directory(directory)) #extend is in place so no assignment

    #create and recycle cursor before the loop
    db_con=create_connection()
    logging.info("Success! Connected to the database")
    cursor=db_con.cursor()

    logging.info(f"Generating schema files for {len(tables)} models")

    for table in tables:
    
        table_path = find_model(table)
        table_path=str(table_path).replace('.sql','.yml')

        with open(table_path,'w') as f:
            f.write("version: 2\n\n")
            f.write("models:\n")
            f.write(f"  - name: {table.lower()}\n")
            f.write("    description:\n")
            f.write("    columns:\n")

            col_query=f"""
            SELECT lower(column_name) as column_name
            FROM COLUMNS
            WHERE lower(table_name) = '{table.lower()}'
            AND lower(TABLE_SCHEMA) = '{os.getenv('SNOWFLAKE_SCHEMA').lower()}'
            ORDER BY ORDINAL_POSITION
            """

            cursor.execute(col_query)
            payload=cursor.fetchall()
            
            for col in payload:
                f.write(f"        - name: {col[0]}\n")
                if doc_lkp(col[0]):
                    f.write(f"          description: '{{{{ doc(\"{col[0]}\") }}}}'\n")
                    # skip the description field if we don't have a doc block
    
    db_con.close()

def main():
    """
    Main function entrypoint
    """
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '-m',
        metavar='--model',
        type=str,
        nargs='+',
        help='name of the base model(s) for schema generation e.g. shopify_orders'
        )

    parser.add_argument(
       '-d',
       metavar='--directory',
       type=str,
       help='name of a models directory'
    )
    
    parser.add_argument(
        '--doc-path',
        type=str,
        help='overrides the definitions markdown file that is traversed for doc blocks',
        default='docs/definitions.md'
    )
    
    args=vars(parser.parse_args())

    if not any([args.get('m'),args.get('d')]):
        parser.error("You must supply either a model or directory of models")

    make_schema(tables=args.get('m'), directory=args.get('d'))

if __name__ == "__main__":
    main()