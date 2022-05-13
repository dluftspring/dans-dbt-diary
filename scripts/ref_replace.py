import os
import sys
from colorama import Fore
import re
import click
from pathlib import Path
from typing import List

def get_file_paths_with_matches(directories: List[Path], pattern: str) -> List[Path]:
    """
    Returns all files with a matching model {{ ref() }} block
    """

    if isinstance(directories, str):
        directories = [directories]

    directories = map(Path, directories)

    files = []
    for directory in directories:
        for file in directory.glob('**/*.sql'):
            matches = re.search(pattern, file.read_text(), re.MULTILINE)
            if matches:
                files.append(file)
    
    
    return files

def replace_refs(file: Path, pattern: str, replacement: str) -> None:
    """
    Replaces all instances of a pattern in a file with a replacement
    """
    with open(file, 'r') as f:
        contents = f.read()
        num_matches = len(re.findall(pattern, contents))
        if num_matches:
            print(Fore.GREEN+"OK:"+Fore.WHITE+f"[{num_matches} matche(s) in"+Fore.YELLOW+f" {file.as_posix().split('/')[-1].replace('.sql','')} "+Fore.WHITE+"will be replaced]")
        contents = re.sub(pattern, replacement, contents)
    with open(file, 'w') as f:
        f.write(contents)

def models_to_patterns(*args) -> List[str]:
    """
    Returns a list of regex patterns for each model
    """
    return (f"ref\(\'{model}\'\)" for model in args)

@click.command()
@click.option('--from', '-f', 'f', help='The model to replace')
@click.option('--to', '-t', 't', help='The model to replace with')
@click.option('--dirs', '-d', default='models', help='Directories to search for models', required=True)
def main(dirs: List[str], f: str, t: str) -> None:
    """
    Replace model references in your dbt project
    """

    match_pattern, replace_pattern = models_to_patterns(f, t)
    files = get_file_paths_with_matches(dirs, match_pattern)
    
    if len(files) == 0:
        print(Fore.RED+"FAILED: [No files found with matching model(s)]")
        sys.exit(1)

    print(Fore.GREEN+"OK:"+Fore.WHITE+f"[{len(files)} files with matches!]")

    for file in files:
        replace_refs(file, match_pattern, replace_pattern)
    
    print(Fore.GREEN+"OK:"+Fore.WHITE+f"[{len(files)} files updated!]")
   

if __name__ == "__main__":
    main()
