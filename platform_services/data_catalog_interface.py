from abc import ABC, abstractmethod

class DataCatalog(ABC):
    """
    Abstract base class for a data cataloging platform.
    This defines the contract that any data catalog implementation must follow.
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