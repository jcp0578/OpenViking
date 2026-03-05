# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""Resource management modules for incremental updates."""

from openviking.resource.resource_lock import (
    ResourceLockManager,
    ResourceLockConflictError,
    ResourceLockError,
)
from openviking.resource.diff_detector import (
    DiffDetector,
    DiffResult,
    FileHash,
    ChangeType,
)
from openviking.resource.staging_manager import StagingManager, StagingArea
from openviking.resource.publication_manager import PublicationManager, PublicationResult
from openviking.resource.vector_reuse_manager import (
    VectorReuseManager,
    ReusableSummary,
    ReuseStats,
)
from openviking.resource.incremental_updater import (
    IncrementalUpdater,
    IncrementalUpdateResult,
    UpdateContext,
)

__all__ = [
    "ResourceLockManager",
    "ResourceLockConflictError",
    "ResourceLockError",
    "DiffDetector",
    "DiffResult",
    "FileHash",
    "ChangeType",
    "StagingManager",
    "StagingArea",
    "PublicationManager",
    "PublicationResult",
    "VectorReuseManager",
    "ReusableSummary",
    "ReuseStats",
    "IncrementalUpdater",
    "IncrementalUpdateResult",
    "UpdateContext",
]
