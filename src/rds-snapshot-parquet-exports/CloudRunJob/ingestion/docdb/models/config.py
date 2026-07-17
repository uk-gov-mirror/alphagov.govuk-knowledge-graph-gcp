from dataclasses import dataclass
from typing import Optional


@dataclass
class BackupConfig:
    prefix: str
    match_regex: str


@dataclass
class AppConfig:
    project_id: str
    bucket_name: str
    dataset_id: str
    collection_name: str
    table_name: Optional[str] = None
    dataset_location: str = "europe-west2"
    object_name: Optional[str] = None
    backup: Optional[BackupConfig] = None