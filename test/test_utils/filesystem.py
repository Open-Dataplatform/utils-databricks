from pathlib import Path
from dataclasses import dataclass
from os import listdir
from typing import Any
@dataclass
class FileInfo:
    path: str
    name: str
    size: int
    modificationTime: float
    
    def __str__(self) -> str:
        return f"FileInfo(path={self.path}, name={self.name},"\
            f"size={self.size}, modificationTime={self.modificationTime})"

    def __hash__(self) -> int:
        return hash((self.path, self.name, self.size, self.modificationTime))
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            raise TypeError(f"incompatible types {type(other)} {type(self)}")
        return hash(self) == hash(other)

class fs:
    def __init__(self):
        pass

    @staticmethod       
    def ls(init_path: str):
        if isinstance(init_path, str):
            path: Path = Path(init_path)
            if not path.exists():
                raise Exception("java.io.FileNotFoundException")
            elif path.is_file():
                return [fs._path_to_fileinfo(path)]
            elif path.is_dir():
                dir_content: list[str] = listdir(str(path))
                return [fs._path_to_fileinfo(Path(content)) for content in dir_content]
            else:
                raise RuntimeError("Unknown error.")
        else:
            raise RuntimeError(f"path should be a string but received {type(init_path)}")

    @staticmethod
    def _path_to_fileinfo(path: Path) -> FileInfo:
        if path.is_file():
            path = path.expanduser().resolve()
            name: str = path.expanduser().resolve().parts[0]
            size: int = path.stat().st_size
            modificationTime: float = path.stat().st_mtime
            return FileInfo(path=str(path), name=name, size=size, modificationTime=modificationTime)

    @staticmethod
    def mkdirs(path: str):
        Path(path).mkdir(parents=True, exist_ok=True)
