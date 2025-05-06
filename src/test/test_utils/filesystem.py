from pathlib import Path
from dataclasses import dataclass

@dataclass
class FileInfo:
    path: str
    name: str
    size: int
    modificationTime: int
    
    def __str__(self):
        return f"FileInfo(path={self.path}, name={self.name},"\
            f"size={self.size}, modificationTime={self.modificationTime})"

class fs:
    def __init__(self):
        pass

    @staticmethod       
    def ls(path: str):
        assert isinstance(path), f"path should be a string, but received value of type {type(path)}"
        path: Path = Path(path)
        if path.is_file():

            return [fileinfo]
        elif path.is_dir
    
    def _path_to_fileinfo(path: Path) -> FileInfo:
        name: str = path.expanduser().resolve().parts[0]
        size: int = path.stat().st_size
        modificationTime: int = path.stat().st_mtime
                    
        return FileInfo(path=str(path), name=name, size=size, modificationTime=modificationTime)
