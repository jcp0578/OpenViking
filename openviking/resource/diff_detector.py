# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Content hash calculation and diff detection for incremental updates.

Implements Merkle Tree-based content hashing and diff detection to identify
changes between resource versions.
"""

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Set

from openviking_cli.utils import get_logger
from openviking_cli.utils.uri import VikingURI

logger = get_logger(__name__)


class ChangeType(Enum):
    """Type of change detected."""
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    UNCHANGED = "unchanged"


@dataclass
class FileHash:
    """Hash information for a file."""
    
    path: str
    content_hash: str
    size: int
    is_directory: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "content_hash": self.content_hash,
            "size": self.size,
            "is_directory": self.is_directory,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FileHash":
        return cls(**data)


@dataclass
class DiffResult:
    """Result of diff detection between two versions."""
    
    added_files: List[FileHash] = field(default_factory=list)
    modified_files: List[FileHash] = field(default_factory=list)
    deleted_files: List[FileHash] = field(default_factory=list)
    unchanged_files: List[FileHash] = field(default_factory=list)
    added_directories: List[FileHash] = field(default_factory=list)
    modified_directories: List[FileHash] = field(default_factory=list)
    deleted_directories: List[FileHash] = field(default_factory=list)
    unchanged_directories: List[FileHash] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "added_files": [f.to_dict() for f in self.added_files],
            "modified_files": [f.to_dict() for f in self.modified_files],
            "deleted_files": [f.to_dict() for f in self.deleted_files],
            "unchanged_files": [f.to_dict() for f in self.unchanged_files],
            "added_directories": [d.to_dict() for d in self.added_directories],
            "modified_directories": [d.to_dict() for d in self.modified_directories],
            "deleted_directories": [d.to_dict() for d in self.deleted_directories],
            "unchanged_directories": [d.to_dict() for d in self.unchanged_directories],
            "stats": self.get_stats(),
        }
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about the diff."""
        return {
            "added_files": len(self.added_files),
            "modified_files": len(self.modified_files),
            "deleted_files": len(self.deleted_files),
            "unchanged_files": len(self.unchanged_files),
            "added_directories": len(self.added_directories),
            "modified_directories": len(self.modified_directories),
            "deleted_directories": len(self.deleted_directories),
            "unchanged_directories": len(self.unchanged_directories),
            "total_changes": (
                len(self.added_files) + len(self.modified_files) + len(self.deleted_files) +
                len(self.added_directories) + len(self.modified_directories) + len(self.deleted_directories)
            ),
        }
    
    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return (
            len(self.added_files) > 0 or
            len(self.modified_files) > 0 or
            len(self.deleted_files) > 0 or
            len(self.added_directories) > 0 or
            len(self.modified_directories) > 0 or
            len(self.deleted_directories) > 0
        )
    
    def get_all_changed_paths(self) -> Set[str]:
        """Get all changed file and directory paths."""
        paths = set()
        for f in self.added_files + self.modified_files + self.deleted_files:
            paths.add(f.path)
        for d in self.added_directories + self.modified_directories + self.deleted_directories:
            paths.add(d.path)
        return paths


class DiffDetector:
    """
    Detects differences between resource versions using content hashing.
    
    Uses SHA256 for content hashing with support for chunked reading of large files.
    Implements Merkle Tree structure for efficient diff detection.
    """
    
    CHUNK_SIZE = 65536
    
    def __init__(self, agfs: Any):
        """
        Initialize DiffDetector.
        
        Args:
            agfs: AGFS client instance
        """
        self._agfs = agfs
    
    def calculate_file_hash(self, file_path: str) -> Optional[FileHash]:
        """
        Calculate SHA256 hash of a file.
        
        Args:
            file_path: Path to the file in AGFS
            
        Returns:
            FileHash object or None if file doesn't exist or is a directory
        """
        try:
            if not self._agfs.exists(file_path):
                return None
            
            if self._agfs.isdir(file_path):
                return None
            
            sha256_hash = hashlib.sha256()
            size = 0
            
            with self._agfs.open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(self.CHUNK_SIZE), b''):
                    sha256_hash.update(chunk)
                    size += len(chunk)
            
            return FileHash(
                path=file_path,
                content_hash=sha256_hash.hexdigest(),
                size=size,
                is_directory=False,
            )
            
        except Exception as e:
            logger.error(f"Failed to calculate hash for {file_path}: {e}")
            return None
    
    def calculate_directory_hash(self, dir_path: str) -> Optional[FileHash]:
        """
        Calculate hash for a directory based on its contents.
        
        The directory hash is computed from the sorted list of child hashes,
        creating a Merkle Tree structure.
        
        Args:
            dir_path: Path to the directory in AGFS
            
        Returns:
            FileHash object or None if directory doesn't exist
        """
        try:
            if not self._agfs.exists(dir_path):
                return None
            
            if not self._agfs.isdir(dir_path):
                return None
            
            child_hashes = []
            
            for child_name in sorted(self._agfs.listdir(dir_path)):
                child_path = f"{dir_path}/{child_name}"
                
                if self._agfs.isdir(child_path):
                    child_hash = self.calculate_directory_hash(child_path)
                else:
                    child_hash = self.calculate_file_hash(child_path)
                
                if child_hash:
                    child_hashes.append(f"{child_name}:{child_hash.content_hash}")
            
            combined_hash = hashlib.sha256("\n".join(child_hashes).encode('utf-8')).hexdigest()
            
            return FileHash(
                path=dir_path,
                content_hash=combined_hash,
                size=0,
                is_directory=True,
            )
            
        except Exception as e:
            logger.error(f"Failed to calculate directory hash for {dir_path}: {e}")
            return None
    
    def collect_resource_hashes(self, resource_uri: str) -> Dict[str, FileHash]:
        """
        Collect hashes for all files and directories in a resource.
        
        Args:
            resource_uri: Resource URI (e.g., "viking://default/resources/my-repo")
            
        Returns:
            Dictionary mapping paths to FileHash objects
        """
        hashes = {}
        
        try:
            viking_uri = VikingURI(resource_uri)
            root_path = viking_uri.local_path
            
            if not self._agfs.exists(root_path):
                logger.warning(f"Resource path does not exist: {root_path}")
                return hashes
            
            self._collect_hashes_recursive(root_path, hashes)
            
            logger.info(
                f"Collected hashes for {resource_uri}: "
                f"{len([h for h in hashes.values() if not h.is_directory])} files, "
                f"{len([h for h in hashes.values() if h.is_directory])} directories"
            )
            
            return hashes
            
        except Exception as e:
            logger.error(f"Failed to collect hashes for {resource_uri}: {e}")
            return hashes
    
    def _collect_hashes_recursive(self, path: str, hashes: Dict[str, FileHash]) -> None:
        """Recursively collect hashes for a path."""
        try:
            if self._agfs.isdir(path):
                dir_hash = self.calculate_directory_hash(path)
                if dir_hash:
                    hashes[path] = dir_hash
                
                for child_name in self._agfs.listdir(path):
                    child_path = f"{path}/{child_name}"
                    self._collect_hashes_recursive(child_path, hashes)
            else:
                file_hash = self.calculate_file_hash(path)
                if file_hash:
                    hashes[path] = file_hash
                    
        except Exception as e:
            logger.error(f"Failed to collect hash for {path}: {e}")
    
    def detect_diff(
        self,
        old_hashes: Dict[str, FileHash],
        new_hashes: Dict[str, FileHash],
    ) -> DiffResult:
        """
        Detect differences between old and new version hashes.
        
        Args:
            old_hashes: Hashes from the old version
            new_hashes: Hashes from the new version
            
        Returns:
            DiffResult with categorized changes
        """
        result = DiffResult()
        
        old_paths = set(old_hashes.keys())
        new_paths = set(new_hashes.keys())
        
        added_paths = new_paths - old_paths
        deleted_paths = old_paths - new_paths
        common_paths = old_paths & new_paths
        
        for path in added_paths:
            file_hash = new_hashes[path]
            if file_hash.is_directory:
                result.added_directories.append(file_hash)
            else:
                result.added_files.append(file_hash)
        
        for path in deleted_paths:
            file_hash = old_hashes[path]
            if file_hash.is_directory:
                result.deleted_directories.append(file_hash)
            else:
                result.deleted_files.append(file_hash)
        
        for path in common_paths:
            old_hash = old_hashes[path]
            new_hash = new_hashes[path]
            
            if old_hash.content_hash == new_hash.content_hash:
                if new_hash.is_directory:
                    result.unchanged_directories.append(new_hash)
                else:
                    result.unchanged_files.append(new_hash)
            else:
                if new_hash.is_directory:
                    result.modified_directories.append(new_hash)
                else:
                    result.modified_files.append(new_hash)
        
        stats = result.get_stats()
        logger.info(
            f"Diff detected: "
            f"added={stats['added_files']} files, {stats['added_directories']} dirs; "
            f"modified={stats['modified_files']} files, {stats['modified_directories']} dirs; "
            f"deleted={stats['deleted_files']} files, {stats['deleted_directories']} dirs; "
            f"unchanged={stats['unchanged_files']} files, {stats['unchanged_directories']} dirs"
        )
        
        return result
    
    def detect_directories_needing_update(
        self,
        diff_result: DiffResult,
        all_hashes: Dict[str, FileHash],
    ) -> Set[str]:
        """
        Identify directories that need summary updates due to changes in descendants.
        
        This implements the "bubble-up" mechanism: when a file or directory changes,
        all ancestor directories need their summaries regenerated.
        
        Args:
            diff_result: Diff detection result
            all_hashes: All hashes from the new version
            
        Returns:
            Set of directory paths that need updates
        """
        changed_paths = diff_result.get_all_changed_paths()
        directories_needing_update = set()
        
        for changed_path in changed_paths:
            path_parts = PurePath(changed_path).parts
            
            for i in range(len(path_parts)):
                ancestor_path = "/".join(path_parts[:i+1])
                
                if ancestor_path in all_hashes and all_hashes[ancestor_path].is_directory:
                    directories_needing_update.add(ancestor_path)
        
        logger.info(
            f"Directories needing update due to bubble-up: {len(directories_needing_update)}"
        )
        
        return directories_needing_update
    
    def get_unchanged_directories(
        self,
        diff_result: DiffResult,
        all_hashes: Dict[str, FileHash],
    ) -> Set[str]:
        """
        Get directories that are completely unchanged (including all descendants).
        
        These directories can have their summaries reused without regeneration.
        
        Args:
            diff_result: Diff detection result
            all_hashes: All hashes from the new version
            
        Returns:
            Set of directory paths that are completely unchanged
        """
        directories_needing_update = self.detect_directories_needing_update(diff_result, all_hashes)
        
        unchanged_directories = set()
        for path, file_hash in all_hashes.items():
            if file_hash.is_directory and path not in directories_needing_update:
                unchanged_directories.add(path)
        
        logger.info(
            f"Unchanged directories (can reuse summaries): {len(unchanged_directories)}"
        )
        
        return unchanged_directories
