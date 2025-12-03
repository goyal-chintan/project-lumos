from abc import ABC, abstractmethod
import warnings

class DataCatalog(ABC):
    """
    Abstract base class for a data cataloging platform.
    This defines the contract that any data catalog implementation must follow.
    
    Deprecated: Prefer using `platform_services.metadata_platform_interface.MetadataPlatformInterface`
    and obtaining concrete platform handlers via `platform_services.platform_factory.PlatformFactory`.
    This interface remains for backward compatibility and may be removed in a future release.
    """

    @abstractmethod
    def emit(self, mce):
        """
        Emits a Metadata Change Event to the data catalog.
        """
        pass

    @abstractmethod
    def emit_mcp(self, mcp):
        """
        Emits a Metadata Change Proposal to the data catalog.
        """
        pass

    @abstractmethod
    def get_emitter(self):
        """
        Returns the underlying emitter object.
        This can be used for platform-specific operations not covered by the interface.
        """
        pass