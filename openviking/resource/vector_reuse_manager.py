# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
"""
Summary and vector reuse strategy for incremental updates.

Implements strategies to reuse summaries and vectors from unchanged files,
avoiding redundant LLM calls and vectorization.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from openviking.resource.diff_detector import DiffResult, FileHash
from openviking.storage.viking_vector_index_backend import VikingVectorIndexBackend
from openviking_cli.utils import get_logger
from openviking_cli.utils.uri import VikingURI

logger = get_logger(__name__)


@dataclass
class ReusableSummary:
    """Summary that can be reused from a previous version."""
    
    file_path: str
    uri: str
    summary: str
    content_hash: str
    vector_id: Optional[str] = None
    vector: Optional[List[float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "uri": self.uri,
            "summary": self.summary,
            "content_hash": self.content_hash,
            "vector_id": self.vector_id,
            "has_vector": self.vector is not None,
            "metadata": self.metadata,
        }


@dataclass
class ReuseStats:
    """Statistics for reuse operations."""
    
    total_files: int = 0
    reused_summaries: int = 0
    reused_vectors: int = 0
    new_summaries: int = 0
    new_vectors: int = 0
    deleted_vectors: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_files": self.total_files,
            "reused_summaries": self.reused_summaries,
            "reused_vectors": self.reused_vectors,
            "new_summaries": self.new_summaries,
            "new_vectors": self.new_vectors,
            "deleted_vectors": self.deleted_vectors,
            "reuse_rate_summaries": (
                self.reused_summaries / self.total_files * 100
                if self.total_files > 0 else 0
            ),
            "reuse_rate_vectors": (
                self.reused_vectors / self.total_files * 100
                if self.total_files > 0 else 0
            ),
        }


class VectorReuseManager:
    """
    Manages summary and vector reuse for incremental updates.
    
    Works with VikingVectorIndexBackend to:
    - Query existing vector records by URI prefix
    - Extract summary information from vector records
    - Identify reusable summaries and vectors
    - Clean up deleted file vectors
    """
    
    BATCH_SIZE = 100
    
    def __init__(self, vector_backend: VikingVectorIndexBackend):
        """
        Initialize VectorReuseManager.
        
        Args:
            vector_backend: VikingVectorIndexBackend instance
        """
        self._backend = vector_backend
    
    async def fetch_vectors_by_uri_prefix(
        self,
        uri_prefix: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all vector records with URIs starting with a prefix.
        
        Args:
            uri_prefix: URI prefix to filter by
            limit: Maximum number of records to fetch
            
        Returns:
            List of vector records
        """
        try:
            records = await self._backend.filter(
                filter={"op": "must", "field": "uri", "conds": [uri_prefix]},
                limit=limit,
            )
            
            logger.info(
                f"Fetched {len(records)} vector records with URI prefix: {uri_prefix}"
            )
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to fetch vectors by URI prefix {uri_prefix}: {e}")
            return []
    
    async def fetch_vectors_by_uris(
        self,
        uris: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch vector records for specific URIs.
        
        Args:
            uris: List of URIs to fetch
            
        Returns:
            Dictionary mapping URIs to vector records
        """
        if not uris:
            return {}
        
        uri_to_record = {}
        
        try:
            for i in range(0, len(uris), self.BATCH_SIZE):
                batch_uris = uris[i:i + self.BATCH_SIZE]
                
                records = await self._backend.filter(
                    filter={"op": "must", "field": "uri", "conds": batch_uris},
                    limit=len(batch_uris) * 2,
                )
                
                for record in records:
                    uri = record.get("uri")
                    if uri:
                        uri_to_record[uri] = record
            
            logger.info(
                f"Fetched {len(uri_to_record)} vector records for {len(uris)} URIs"
            )
            
            return uri_to_record
            
        except Exception as e:
            logger.error(f"Failed to fetch vectors by URIs: {e}")
            return {}
    
    def extract_summary_from_record(
        self,
        record: Dict[str, Any],
    ) -> Optional[ReusableSummary]:
        """
        Extract summary information from a vector record.
        
        Args:
            record: Vector record from the backend
            
        Returns:
            ReusableSummary if extraction succeeds, None otherwise
        """
        try:
            uri = record.get("uri")
            if not uri:
                return None
            
            viking_uri = VikingURI(uri)
            file_path = viking_uri.local_path
            
            summary = record.get("summary", "")
            content_hash = record.get("content_hash", "")
            
            if not summary:
                return None
            
            return ReusableSummary(
                file_path=file_path,
                uri=uri,
                summary=summary,
                content_hash=content_hash,
                vector_id=record.get("id"),
                vector=record.get("vector"),
                metadata={
                    "level": record.get("level"),
                    "context_type": record.get("context_type"),
                    "parent_uri": record.get("parent_uri"),
                    "file_name": record.get("file_name"),
                },
            )
            
        except Exception as e:
            logger.error(f"Failed to extract summary from record: {e}")
            return None
    
    async def identify_reusable_summaries(
        self,
        unchanged_files: List[FileHash],
        old_uri_prefix: str,
    ) -> Dict[str, ReusableSummary]:
        """
        Identify summaries that can be reused from unchanged files.
        
        Args:
            unchanged_files: List of unchanged file hashes
            old_uri_prefix: URI prefix for the old version
            
        Returns:
            Dictionary mapping file paths to reusable summaries
        """
        if not unchanged_files:
            return {}
        
        reusable_summaries = {}
        
        try:
            old_uris = []
            path_to_file_hash = {}
            
            for file_hash in unchanged_files:
                old_uri = file_hash.path.replace(
                    file_hash.path.split("/resources/")[0],
                    old_uri_prefix.rsplit("/", 1)[0]
                )
                old_uris.append(old_uri)
                path_to_file_hash[file_hash.path] = file_hash
            
            old_records = await self.fetch_vectors_by_uris(old_uris)
            
            for old_uri, record in old_records.items():
                summary = self.extract_summary_from_record(record)
                if summary:
                    viking_uri = VikingURI(old_uri)
                    new_uri = old_uri.replace(
                        old_uri_prefix.rsplit("/", 1)[0],
                        viking_uri.scheme + "://" + viking_uri.authority
                    )
                    
                    summary.uri = new_uri
                    reusable_summaries[summary.file_path] = summary
            
            logger.info(
                f"Identified {len(reusable_summaries)} reusable summaries "
                f"from {len(unchanged_files)} unchanged files"
            )
            
            return reusable_summaries
            
        except Exception as e:
            logger.error(f"Failed to identify reusable summaries: {e}")
            return {}
    
    async def delete_vectors_for_files(
        self,
        deleted_files: List[FileHash],
        old_uri_prefix: str,
    ) -> int:
        """
        Delete vector records for deleted files.
        
        Args:
            deleted_files: List of deleted file hashes
            old_uri_prefix: URI prefix for the old version
            
        Returns:
            Number of deleted records
        """
        if not deleted_files:
            return 0
        
        total_deleted = 0
        
        try:
            for file_hash in deleted_files:
                old_uri = file_hash.path.replace(
                    file_hash.path.split("/resources/")[0],
                    old_uri_prefix.rsplit("/", 1)[0]
                )
                
                deleted = await self._backend.remove_by_uri(old_uri)
                total_deleted += deleted
            
            logger.info(
                f"Deleted {total_deleted} vector records for {len(deleted_files)} deleted files"
            )
            
            return total_deleted
            
        except Exception as e:
            logger.error(f"Failed to delete vectors for files: {e}")
            return total_deleted
    
    async def prepare_reuse_plan(
        self,
        diff_result: DiffResult,
        old_resource_uri: str,
        new_resource_uri: str,
    ) -> Dict[str, Any]:
        """
        Prepare a complete reuse plan for incremental update.
        
        Args:
            diff_result: Diff detection result
            old_resource_uri: Old resource URI
            new_resource_uri: New resource URI
            
        Returns:
            Reuse plan with statistics and reusable summaries
        """
        stats = ReuseStats()
        reusable_summaries = {}
        
        try:
            stats.total_files = (
                len(diff_result.unchanged_files) +
                len(diff_result.added_files) +
                len(diff_result.modified_files)
            )
            
            reusable_summaries = await self.identify_reusable_summaries(
                diff_result.unchanged_files,
                old_resource_uri,
            )
            stats.reused_summaries = len(reusable_summaries)
            stats.reused_vectors = sum(
                1 for s in reusable_summaries.values() if s.vector is not None
            )
            
            stats.new_summaries = (
                len(diff_result.added_files) +
                len(diff_result.modified_files)
            )
            stats.new_vectors = stats.new_summaries
            
            stats.deleted_vectors = await self.delete_vectors_for_files(
                diff_result.deleted_files,
                old_resource_uri,
            )
            
            logger.info(
                f"Reuse plan prepared: "
                f"total_files={stats.total_files}, "
                f"reused_summaries={stats.reused_summaries} ({stats.to_dict()['reuse_rate_summaries']:.1f}%), "
                f"reused_vectors={stats.reused_vectors} ({stats.to_dict()['reuse_rate_vectors']:.1f}%), "
                f"new_summaries={stats.new_summaries}, "
                f"deleted_vectors={stats.deleted_vectors}"
            )
            
            return {
                "stats": stats.to_dict(),
                "reusable_summaries": {
                    path: summary.to_dict()
                    for path, summary in reusable_summaries.items()
                },
            }
            
        except Exception as e:
            logger.error(f"Failed to prepare reuse plan: {e}")
            return {
                "stats": stats.to_dict(),
                "reusable_summaries": {},
                "error": str(e),
            }
