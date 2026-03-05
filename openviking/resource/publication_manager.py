# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Atomic publication management for incremental updates.

Implements atomic publication flow: file system switch + vector index switch.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from openviking.resource.staging_manager import StagingArea
from openviking.storage.viking_vector_index_backend import VikingVectorIndexBackend
from openviking_cli.utils import get_logger
from openviking_cli.utils.uri import VikingURI

logger = get_logger(__name__)


@dataclass
class PublicationResult:
    """Result of a publication operation."""
    
    success: bool
    target_uri: str
    fs_switched: bool = False
    vector_switched: bool = False
    corrupted: bool = False
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "target_uri": self.target_uri,
            "fs_switched": self.fs_switched,
            "vector_switched": self.vector_switched,
            "corrupted": self.corrupted,
            "error_message": self.error_message,
        }


class PublicationManager:
    """
    Manages atomic publication of resource updates.
    
    Implements the atomic publication flow:
    1. File system switch: delete old directory + move staging directory
    2. Vector index switch: delete old vectors + upsert new vectors
    3. Failure handling: create .corrupted marker if vector switch fails
    """
    
    CORRUPTED_MARKER = ".corrupted"
    
    def __init__(self, agfs: Any, vector_backend: VikingVectorIndexBackend):
        """
        Initialize PublicationManager.
        
        Args:
            agfs: AGFS client instance
            vector_backend: VikingVectorIndexBackend instance
        """
        self._agfs = agfs
        self._vector_backend = vector_backend
    
    def _create_corrupted_marker(self, resource_path: str) -> None:
        """Create .corrupted marker file."""
        try:
            marker_path = f"{resource_path}/{self.CORRUPTED_MARKER}"
            self._agfs.write(marker_path, b"corrupted")
            logger.warning(f"Created corrupted marker: {marker_path}")
        except Exception as e:
            logger.error(f"Failed to create corrupted marker: {e}")
    
    def _check_corrupted_marker(self, resource_path: str) -> bool:
        """Check if .corrupted marker exists."""
        try:
            marker_path = f"{resource_path}/{self.CORRUPTED_MARKER}"
            return self._agfs.exists(marker_path)
        except Exception as e:
            logger.error(f"Failed to check corrupted marker: {e}")
            return False
    
    def _remove_corrupted_marker(self, resource_path: str) -> None:
        """Remove .corrupted marker file."""
        try:
            marker_path = f"{resource_path}/{self.CORRUPTED_MARKER}"
            if self._agfs.exists(marker_path):
                self._agfs.rm(marker_path)
                logger.info(f"Removed corrupted marker: {marker_path}")
        except Exception as e:
            logger.error(f"Failed to remove corrupted marker: {e}")
    
    async def switch_filesystem(
        self,
        staging_area: StagingArea,
    ) -> bool:
        """
        Perform atomic file system switch.
        
        Steps:
        1. Delete old target directory (if exists)
        2. Move staging directory to target location
        
        Args:
            staging_area: StagingArea object
            
        Returns:
            True if switch succeeds, False otherwise
        """
        try:
            target_path = staging_area.target_path
            staging_path = staging_area.staging_path
            
            if self._agfs.exists(target_path):
                logger.info(f"Removing old target directory: {target_path}")
                self._agfs.rm(target_path, recursive=True)
            
            parent_dir = target_path.rsplit("/", 1)[0]
            if not self._agfs.exists(parent_dir):
                self._agfs.mkdir(parent_dir, parents=True)
            
            logger.info(
                f"Moving staging directory: {staging_path} -> {target_path}"
            )
            self._agfs.mv(staging_path, target_path)
            
            logger.info(f"File system switch completed: {target_path}")
            return True
            
        except Exception as e:
            logger.error(f"File system switch failed: {e}")
            return False
    
    async def switch_vector_index(
        self,
        old_resource_uri: str,
        new_vectors: List[Dict[str, Any]],
    ) -> bool:
        """
        Perform vector index switch using delete + upsert.
        
        Steps:
        1. Delete all vectors for old resource URI
        2. Upsert new vectors
        
        Args:
            old_resource_uri: Old resource URI
            new_vectors: List of new vector records to upsert
            
        Returns:
            True if switch succeeds, False otherwise
        """
        try:
            logger.info(f"Deleting old vectors for: {old_resource_uri}")
            deleted_count = await self._vector_backend.remove_by_uri(old_resource_uri)
            logger.info(f"Deleted {deleted_count} old vector records")
            
            logger.info(f"Upserting {len(new_vectors)} new vector records")
            upserted_count = 0
            for vector_data in new_vectors:
                try:
                    await self._vector_backend.upsert(vector_data)
                    upserted_count += 1
                except Exception as e:
                    logger.warning(f"Failed to upsert vector: {e}")
            
            logger.info(
                f"Vector index switch completed: "
                f"deleted={deleted_count}, upserted={upserted_count}"
            )
            return True
            
        except Exception as e:
            logger.error(f"Vector index switch failed: {e}")
            return False
    
    async def publish(
        self,
        staging_area: StagingArea,
        old_resource_uri: Optional[str] = None,
        new_vectors: Optional[List[Dict[str, Any]]] = None,
    ) -> PublicationResult:
        """
        Perform complete atomic publication.
        
        Args:
            staging_area: StagingArea object
            old_resource_uri: Old resource URI (for vector deletion)
            new_vectors: List of new vector records to upsert
            
        Returns:
            PublicationResult object
        """
        result = PublicationResult(
            success=False,
            target_uri=staging_area.target_uri,
        )
        
        try:
            fs_success = await self.switch_filesystem(staging_area)
            if not fs_success:
                result.error_message = "File system switch failed"
                return result
            
            result.fs_switched = True
            
            if old_resource_uri and new_vectors:
                vector_success = await self.switch_vector_index(
                    old_resource_uri,
                    new_vectors,
                )
                
                if not vector_success:
                    result.error_message = "Vector index switch failed"
                    result.corrupted = True
                    self._create_corrupted_marker(staging_area.target_path)
                    return result
                
                result.vector_switched = True
            else:
                logger.info("Skipping vector index switch (no vectors provided)")
                result.vector_switched = True
            
            self._remove_corrupted_marker(staging_area.target_path)
            
            result.success = True
            logger.info(
                f"Publication completed successfully: target_uri={staging_area.target_uri}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Publication failed: {e}")
            result.error_message = str(e)
            
            if result.fs_switched and not result.vector_switched:
                result.corrupted = True
                self._create_corrupted_marker(staging_area.target_path)
            
            return result
    
    def is_resource_corrupted(self, resource_uri: str) -> bool:
        """
        Check if a resource is marked as corrupted.
        
        Args:
            resource_uri: Resource URI to check
            
        Returns:
            True if resource is corrupted, False otherwise
        """
        try:
            viking_uri = VikingURI(resource_uri)
            resource_path = f"/{viking_uri.full_path}"
            return self._check_corrupted_marker(resource_path)
        except Exception as e:
            logger.error(f"Failed to check corrupted status: {e}")
            return False
    
    def mark_resource_corrupted(self, resource_uri: str) -> bool:
        """
        Mark a resource as corrupted.
        
        Args:
            resource_uri: Resource URI to mark
            
        Returns:
            True if marking succeeds, False otherwise
        """
        try:
            viking_uri = VikingURI(resource_uri)
            resource_path = f"/{viking_uri.full_path}"
            self._create_corrupted_marker(resource_path)
            return True
        except Exception as e:
            logger.error(f"Failed to mark resource as corrupted: {e}")
            return False
    
    def clear_resource_corrupted(self, resource_uri: str) -> bool:
        """
        Clear corrupted marker from a resource.
        
        Args:
            resource_uri: Resource URI to clear
            
        Returns:
            True if clearing succeeds, False otherwise
        """
        try:
            viking_uri = VikingURI(resource_uri)
            resource_path = f"/{viking_uri.full_path}"
            self._remove_corrupted_marker(resource_path)
            return True
        except Exception as e:
            logger.error(f"Failed to clear corrupted marker: {e}")
            return False
