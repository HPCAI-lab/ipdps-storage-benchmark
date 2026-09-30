"""Abstract base class for all database backends."""

from abc import ABC, abstractmethod


class DatabaseBackend(ABC):
    """Standardized interface for all database backends used in benchmarking."""

    @abstractmethod
    def connect(self):
        """Establish connection to the database."""
        ...

    @abstractmethod
    def initialize_schema(self):
        """Create tables / collections / measurements as needed."""
        ...

    @abstractmethod
    def insert_coordinates_batch(self, records: list[dict]):
        """Insert a batch of coordinate records.

        Each record must have keys: file_id, frame, atom_index, x, y, z.
        """
        ...

    @abstractmethod
    def insert_telemetry_batch(self, records: list[dict]):
        """Insert a batch of telemetry records.

        Each record must have keys: ts (datetime), metric, value, tags (dict).
        """
        ...

    @abstractmethod
    def query_coordinates(self, file_id: int) -> list:
        """Return all coordinate rows for *file_id* ordered by (frame, atom_index)."""
        ...

    @abstractmethod
    def query_telemetry_range(self, start, end) -> list:
        """Return all telemetry rows whose ts falls in [start, end]."""
        ...

    @abstractmethod
    def close(self):
        """Release the database connection."""
        ...

    @abstractmethod
    def get_backend_name(self) -> str:
        """Human-readable name for reporting (e.g. 'sqlite', 'mongodb')."""
        ...

    @abstractmethod
    def initialize_scientific_metadata_schema(self):
        """Create the scientific metadata table and indexes."""
        ...

    @abstractmethod
    def insert_scientific_metadata_batch(self, records):
        """Insert and commit one batch of scientific metadata records."""
        ...

    @abstractmethod
    def query_scientific_metadata_range(self, first, last):
        """Return records with first <= record_id < last, ordered by ID."""
        ...
