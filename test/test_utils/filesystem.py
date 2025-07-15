import os
from pathlib import Path
from dataclasses import dataclass
from typing import Any

@dataclass
class FileInfo:
    """Mimics FileInfo object in databricks.
    """
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
    """Mimics fs object in databricks.
    """
    def __init__(self):
        """Empty initializer.
        """
        pass

    @staticmethod       
    def ls(init_path: str) -> list[FileInfo|None]:
        """Mimics ls method in databricks

        Args:
            init_path (str): initial path.

        Raises:
            Exception: Raised to mimic java.io.FileNotFoundException from data bricks.
            RuntimeError: Unknown error.
            RuntimeError: Raised if path is not string.

        Returns:
            list[FileInfo|None]: _description_
        """
        if isinstance(init_path, str):
            path: Path = Path(init_path)
            if not path.exists():
                raise Exception("java.io.FileNotFoundException")
            elif path.is_file():
                return [fs._path_to_fileinfo(path)]
            elif path.is_dir():
                dir_content: list[str] = []
                for root, _, files in os.walk(str(path)):
                    for name in files:
                        dir_content.append(os.path.join(root, name))
                return [fs._path_to_fileinfo(Path(content)) for content in dir_content]
            else:
                raise RuntimeError("Unknown error.")
        else:
            raise TypeError(f"path should be a string but received {type(init_path)}")

    @staticmethod
    def _path_to_fileinfo(path: Path) -> FileInfo|None:
        """Converts path to FileInfo Object.

        Args:
            path (Path): Path of file

        Returns:
            FileInfo|None: File information as in databricks if path exists.
        """
        if path.is_file():
            path = path.expanduser().resolve()
            name: str = path.expanduser().resolve().parts[-1]
            size: int = path.stat().st_size
            modificationTime: float = path.stat().st_mtime
            return FileInfo(path=str(path), name=name, size=size, modificationTime=modificationTime)
        return None

    @staticmethod
    def mkdirs(path: str):
        """Creates directories

        Args:
            path (str): Path to create.
        """
        Path(path).mkdir(parents=True, exist_ok=True)
        
    @staticmethod
    def head(path: str) -> str:
        """Mimics head method in databricks.

        Args:
            path (str): Path to file 

        Returns:
            str: content of file.
        """
        with open(path, "r") as f:
            stream = f.read()
        return stream
