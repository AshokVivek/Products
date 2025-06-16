from typing_extensions import Any


class LoggingContext:
    """
    A class for managing a logging context.

    Attributes:
        store (dict): A dictionary to store logging context key-value pairs.
    """

    # Reference: https://docs.python.org/tr/3.10/library/typing.html 
    def __init__(self, **kwargs: Any) -> 'LoggingContext':
        """
        Initializes a LoggingContext instance.

        Args:
            self,
            **kwargs: Key-value pairs to populate the logging context store.

        Returns:
            LoggingContext instance
        """

        self.store: dict = {}

        for key, value in kwargs.items():
            self.store[key] = value

    def upsert(self, **kwargs: Any) -> None:
        """
        Adds or updates key-value pairs in the logging context store.

        Args:
            self,
            **kwargs: Key-value pairs to add or update.

        Returns:
            None

        Raises:
            ValueError: If self.store does not exist/ is empty.
            TypeError: If self.store is not a dictionary.
        """

        if not self.store:
            raise ValueError("Logging context store not found to be initialized")
        
        if not isinstance(self.store, dict):
            raise TypeError("Logging context store not found to be a dictionary")

        for key, value in kwargs.items():
            self.store[key] = value

    def remove_keys(self, keys) -> None:
        """
        Removes specified keys from the logging context store.

        Args:
            self,
            keys (List[str]): List of keys to remove.

        Returns:
            None

        Raises:
            ValueError: If self.store does not exist/ is empty.
            TypeError: If self.store is not a dictionary.
        """

        if not self.store:
            raise ValueError("Logging context store not found to be initialized")
        
        if not isinstance(self.store, dict):
            raise TypeError("Logging context store not found to be a dictionary")

        for key in keys:
            if key in self.store:
                del self.store[key]

    def clear(self) -> None:
        """
        Clears the current logging context store.

        Args:
            self

        Returns:
            None

        Raises:
            ValueError: If self.store does not exist/ is empty.
            TypeError: If self.store is not a dictionary.
        """

        if not self.store:
            raise ValueError("Logging context store not found to be initialized")
        
        if not isinstance(self.store, dict):
            raise TypeError("Logging context store not found to be a dictionary")
        
        self.store.clear()