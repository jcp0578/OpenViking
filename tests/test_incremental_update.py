# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Basic tests for incremental resource update functionality.
"""

import pytest
from unittest.mock import Mock, MagicMock, AsyncMock
from openviking.resource import (
    ResourceLockManager,
    ResourceLockConflictError,
    DiffDetector,
    DiffResult,
    FileHash,
    ChangeType,
    StagingManager,
    StagingArea,
    PublicationManager,
    VectorReuseManager,
    IncrementalUpdater,
)


class TestResourceLockManager:
    """Tests for ResourceLockManager."""
    
    def test_lock_creation(self):
        """Test lock creation and release."""
        agfs = Mock()
        agfs.exists = Mock(return_value=False)
        agfs.mkdir = Mock()
        agfs.write = Mock()
        agfs.rm = Mock()
        
        manager = ResourceLockManager(agfs)
        lock_info = manager.acquire_lock(
            resource_uri="viking://default/resources/test",
            operation="test_op",
        )
        
        assert lock_info.resource_uri == "viking://default/resources/test"
        assert lock_info.operation == "test_op"
        assert lock_info.lock_id is not None
        
        result = manager.release_lock("viking://default/resources/test", lock_info.lock_id)
        assert result is True
    
    def test_lock_conflict(self):
        """Test lock conflict detection."""
        agfs = Mock()
        agfs.exists = Mock(return_value=True)
        agfs.mkdir = Mock()
        agfs.read = Mock(return_value=b'{"lock_id": "existing-lock", "resource_uri": "viking://default/resources/test", "operation": "other_op", "created_at": 0, "expires_at": null, "metadata": {}}')
        
        manager = ResourceLockManager(agfs)
        
        with pytest.raises(ResourceLockConflictError):
            manager.acquire_lock(
                resource_uri="viking://default/resources/test",
                operation="test_op",
            )


class TestDiffDetector:
    """Tests for DiffDetector."""
    
    def test_file_hash_calculation(self):
        """Test file hash calculation."""
        agfs = Mock()
        agfs.exists = Mock(return_value=True)
        agfs.isdir = Mock(return_value=False)
        agfs.open = MagicMock()
        agfs.open.return_value.__enter__ = Mock(return_value=Mock(read=lambda size: b"test content" if size > 0 else b""))
        agfs.open.return_value.__exit__ = Mock(return_value=False)
        
        detector = DiffDetector(agfs)
        file_hash = detector.calculate_file_hash("/test/file.txt")
        
        assert file_hash is not None
        assert file_hash.path == "/test/file.txt"
        assert file_hash.content_hash is not None
        assert file_hash.size > 0
        assert not file_hash.is_directory
    
    def test_diff_detection(self):
        """Test diff detection between versions."""
        old_hashes = {
            "/test/file1.txt": FileHash(
                path="/test/file1.txt",
                content_hash="hash1",
                size=100,
                is_directory=False,
            ),
            "/test/file2.txt": FileHash(
                path="/test/file2.txt",
                content_hash="hash2",
                size=200,
                is_directory=False,
            ),
        }
        
        new_hashes = {
            "/test/file1.txt": FileHash(
                path="/test/file1.txt",
                content_hash="hash1",
                size=100,
                is_directory=False,
            ),
            "/test/file2.txt": FileHash(
                path="/test/file2.txt",
                content_hash="hash2_modified",
                size=250,
                is_directory=False,
            ),
            "/test/file3.txt": FileHash(
                path="/test/file3.txt",
                content_hash="hash3",
                size=150,
                is_directory=False,
            ),
        }
        
        detector = DiffDetector(Mock())
        diff_result = detector.detect_diff(old_hashes, new_hashes)
        
        assert len(diff_result.unchanged_files) == 1
        assert len(diff_result.modified_files) == 1
        assert len(diff_result.added_files) == 1
        assert len(diff_result.deleted_files) == 0
        assert diff_result.has_changes()


class TestStagingManager:
    """Tests for StagingManager."""
    
    def test_staging_area_creation(self):
        """Test staging area creation."""
        agfs = Mock()
        agfs.exists = Mock(return_value=False)
        agfs.mkdir = Mock()
        
        manager = StagingManager(agfs)
        staging_area = manager.create_staging_area("viking://default/resources/test")
        
        assert staging_area.target_uri == "viking://default/resources/test"
        assert staging_area.staging_id is not None
        assert ".staging" in staging_area.staging_path


class TestPublicationManager:
    """Tests for PublicationManager."""
    
    @pytest.mark.asyncio
    async def test_filesystem_switch(self):
        """Test filesystem switch."""
        agfs = Mock()
        agfs.exists = Mock(return_value=True)
        agfs.rm = Mock()
        agfs.mkdir = Mock()
        agfs.mv = Mock()
        
        vector_backend = Mock()
        
        manager = PublicationManager(agfs, vector_backend)
        
        staging_area = StagingArea(
            staging_uri="viking://default/.staging/test-staging",
            staging_path="/.staging/test-staging",
            target_uri="viking://default/resources/test",
            target_path="/resources/test",
            staging_id="test-staging",
        )
        
        result = await manager.switch_filesystem(staging_area)
        assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
