"""Common type aliases used across the MODERATE workflows codebase.

This module defines type aliases for frequently used type patterns to improve
code readability, consistency, and maintainability. Using type aliases makes
complex types more semantic and easier to refactor.

Type Categories:
- JSON/API response types: JSONDict, JSONList, JSONValue
- Metadata types: MetadataDict, TagsDict
- Resource identifiers: DatabaseName, AssetID, TaskID, UserID
- Configuration types: URLString, NamespaceKey

Example:
    from moderate.types import JSONDict, MetadataDict, AssetID

    def create_asset(name: str, tags: MetadataDict = None) -> JSONDict:
        # Implementation
        return {"id": 123, "name": name}

    def get_asset(asset_id: AssetID) -> JSONDict:
        # Implementation
        pass
"""

from typing import Any, Dict, List, Union

# JSON/API Response Types
JSONDict = Dict[str, Any]
JSONList = List[JSONDict]
JSONValue = Union[str, int, float, bool, None, JSONDict, JSONList]

# Metadata and Tag Types
MetadataDict = Dict[str, str]
TagsDict = Dict[str, str]

# Resource Identifier Types
DatabaseName = str
AssetID = int
TaskID = int
UserID = str
JobID = int

# Configuration Types
URLString = str
NamespaceKey = str
CommitSHA = str

# File and Storage Types
FilePath = str
BucketName = str
S3Key = str
