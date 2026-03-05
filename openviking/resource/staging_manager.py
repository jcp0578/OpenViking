# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Staging area management for incremental updates.

Manages temporary staging URIs for new content before atomic publication.
"""

import uuid
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any, Dict, List, Optional

from openviking_cli.utils import get_logger
from openviking_cli.utils.uri import VikingURI

logger = get_logger(__name__)


@dataclass
class StagingArea:
    """Represents a staging area for new content."""
    
    staging_uri: str
    staging_path: str
    target_uri: str
    target_path: str
    staging_id: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "staging_uri": self.staging_uri,
            "staging_path": self.staging_path,
            "target_uri": self.target_uri,
            "target_path": self.target_path,
            "staging_id": self.staging_id,
        }


class StagingManager:
    """
    Manages temporary staging areas for incremental updates.
    
    Staging areas are used to prepare new content before atomic publication.
    They are created under a special `.staging/` directory in the AGFS root.
    """
    
    STAGING_DIR = ".staging"
    
    def __init__(self, agfs: Any):
        """
        Initialize StagingManager.
        
        Args:
            agfs: AGFS client instance
        """
        self._agfs = agfs
        self._staging_dir_path = f"/{self.STAGING_DIR}"
    
    def _ensure_staging_dir(self) -> None:
        """Ensure staging directory exists."""
        try:
            if not self._agfs.exists(self._staging_dir_path):
                self._agfs.mkdir(self._staging_dir_path)
                logger.info(f"Created staging directory: {self._staging_dir_path}")
        except Exception as e:
            logger.warning(f"Failed to ensure staging directory: {e}")
    
    def create_staging_area(
        self,
        target_uri: str,
        staging_id: Optional[str] = None,
    ) -> StagingArea:
        """
        Create a new staging area for a target URI.
        
        Args:
            target_uri: Target resource URI
            staging_id: Optional staging ID (auto-generated if not provided)
            
        Returns:
            StagingArea object
        """
        self._ensure_staging_dir()
        
        staging_id = staging_id or str(uuid.uuid4())[:8]
        
        viking_uri = VikingURI(target_uri)
        target_path = f"/{viking_uri.full_path}"
        
        staging_path = f"{self._staging_dir_path}/{staging_id}"
        
        staging_uri = f"viking://{self.STAGING_DIR}/{staging_id}"
        
        staging_area = StagingArea(
            staging_uri=staging_uri,
            staging_path=staging_path,
            target_uri=target_uri,
            target_path=target_path,
            staging_id=staging_id,
        )
        
        try:
            if not self._agfs.exists(staging_path):
                self._agfs.mkdir(staging_path)
                logger.info(
                    f"Created staging area: staging_id={staging_id}, "
                    f"staging_path={staging_path}, target_uri={target_uri}"
                )
        except Exception as e:
            logger.error(f"Failed to create staging area: {e}")
            raise
        
        return staging_area
    
    def copy_to_staging(
        self,
        staging_area: StagingArea,
        source_path: str,
        relative_path: str,
    ) -> bool:
        """
        Copy a file or directory to the staging area.
        
        Args:
            staging_area: StagingArea object
            source_path: Source path in AGFS
            relative_path: Relative path within the staging area
            
        Returns:
            True if copy succeeds, False otherwise
        """
        try:
            dest_path = f"{staging_area.staging_path}/{relative_path}"
            
            parent_dir = str(PurePath(dest_path).parent)
            if not self._agfs.exists(parent_dir):
                self._agfs.mkdir(parent_dir, parents=True)
            
            if self._agfs.isdir(source_path):
                self._copy_directory_recursive(source_path, dest_path)
            else:
                self._copy_file(source_path, dest_path)
            
            logger.debug(f"Copied {source_path} to {dest_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy {source_path} to staging: {e}")
            return False
    
    def _copy_file(self, source: str, dest: str) -> None:
        """Copy a single file."""
        data = self._agfs.read(source)
        self._agfs.write(dest, data)
    
    def _copy_directory_recursive(self, source: str, dest: str) -> None:
        """Recursively copy a directory."""
        if not self._agfs.exists(dest):
            self._agfs.mkdir(dest)
        
        for item in self._agfs.listdir(source):
            source_item = f"{source}/{item}"
            dest_item = f"{dest}/{item}"
            
            if self._agfs.isdir(source_item):
                self._copy_directory_recursive(source_item, dest_item)
            else:
                self._copy_file(source_item, dest_item)
    
    def upload_to_staging(
        self,
        staging_area: StagingArea,
        local_path: str,
        relative_path: str,
    ) -> bool:
        """
        Upload a local file or directory to the staging area.
        
        Args:
            staging_area: StagingArea object
            local_path: Local file system path
            relative_path: Relative path within the staging area
            
        Returns:
            True if upload succeeds, False otherwise
        """
        try:
            dest_path = f"{staging_area.staging_path}/{relative_path}"
            
            import os
            if os.path.isdir(local_path):
                self._upload_directory_recursive(local_path, dest_path)
            else:
                self._upload_file(local_path, dest_path)
            
            logger.debug(f"Uploaded {local_path} to {dest_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload {local_path} to staging: {e}")
            return False
    
    def _upload_file(self, local_path: str, dest_path: str) -> None:
        """Upload a single file."""
        parent_dir = str(PurePath(dest_path).parent)
        if not self._agfs.exists(parent_dir):
            self._agfs.mkdir(parent_dir, parents=True)
        
        with open(local_path, 'rb') as f:
            data = f.read()
        self._agfs.write(dest_path, data)
    
    def _upload_directory_recursive(self, local_path: str, dest_path: str) -> None:
        """Recursively upload a directory."""
        import os
        
        if not self._agfs.exists(dest_path):
            self._agfs.mkdir(dest_path)
        
        for item in os.listdir(local_path):
            local_item = os.path.join(local_path, item)
            dest_item = f"{dest_path}/{item}"
            
            if os.path.isdir(local_item):
                self._upload_directory_recursive(local_item, dest_item)
            else:
                self._upload_file(local_item, dest_item)
    
    def cleanup_staging_area(self, staging_area: StagingArea) -> bool:
        """
        Clean up a staging area.
        
        Args:
            staging_area: StagingArea object
            
        Returns:
            True if cleanup succeeds, False otherwise
        """
        try:
            if self._agfs.exists(staging_area.staging_path):
                self._agfs.rm(staging_area.staging_path, recursive=True)
                logger.info(
                    f"Cleaned up staging area: staging_id={staging_area.staging_id}"
                )
            return True
            
        except Exception as e:
            logger.error(f"Failed to cleanup staging area: {e}")
            return False
    
    def cleanup_all_staging_areas(self) -> int:
        """
        Clean up all staging areas (for service restart).
        
        Returns:
            Number of staging areas cleaned up
        """
        cleaned = 0
        
        try:
            if not self._agfs.exists(self._staging_dir_path):
                return 0
            
            staging_areas = self._agfs.listdir(self._staging_dir_path)
            
            for staging_id in staging_areas:
                staging_path = f"{self._staging_dir_path}/{staging_id}"
                self._agfs.rm(staging_path, recursive=True)
                cleaned += 1
            
            if cleaned > 0:
                logger.info(f"Cleaned up {cleaned} staging areas on service restart")
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Failed to cleanup all staging areas: {e}")
            return cleaned
    
    def list_staging_areas(self) -> List[str]:
        """
        List all existing staging areas.
        
        Returns:
            List of staging IDs
        """
        try:
            if not self._agfs.exists(self._staging_dir_path):
                return []
            
            return self._agfs.listdir(self._staging_dir_path)
            
        except Exception as e:
            logger.error(f"Failed to list staging areas: {e}")
            return []
