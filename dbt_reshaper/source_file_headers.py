from .dynamic_modules import DynamicModules
from typing import Optional
import dbt
from dbt.contracts.files import AnySourceFile, FilePath, ParseFileType, FileHash
from dbt.parser.read_files import load_source_file

load_source_file_original =  dbt.parser.read_files.load_source_file

def load_source_file_override(
    path: FilePath,
    parse_file_type: ParseFileType,
    project_name: str,
    saved_files,
) -> Optional[AnySourceFile]:
  source_file = load_source_file_original(path, parse_file_type, project_name, saved_files)

  if source_file is not None:
    if str(parse_file_type) in DynamicModules.headers:
      source_file.contents = DynamicModules.headers[str(parse_file_type)] + '\n' + source_file.contents
      source_file.checksum = FileHash.from_contents(source_file.contents)

  return source_file

dbt.parser.read_files.load_source_file = load_source_file_override