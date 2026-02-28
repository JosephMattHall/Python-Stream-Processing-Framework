import importlib.metadata
from typing import Dict, Type, Optional, Callable
from pspf.utils.logging import get_logger

logger = get_logger("PluginManager")

class PluginManager:
    """
    Manages loading and resolving external plugins for PSPF.

    Plugins can register Connectors, StateStores, or Serializers utilizing 
    Python's standard `entry_points` architecture.

    Packages should declare:
    [project.entry-points."pspf.plugins"]
    my_plugin = "my_package.plugin:register"
    """

    def __init__(self) -> None:
        self._backends: Dict[str, Type] = {}
        self._state_stores: Dict[str, Type] = {}
        self._loaded = False

    def load_plugins(self) -> None:
        """
        Discover and load all plugins registering under 'pspf.plugins' entry point.
        """
        if self._loaded:
            return

        try:
            # Python 3.10+
            entry_points = importlib.metadata.entry_points(group='pspf.plugins')
        except TypeError:
            # Fallback for older versions
            eps = importlib.metadata.entry_points()
            entry_points = eps.get('pspf.plugins', []) # type: ignore

        for ep in entry_points: # type: ignore
            try:
                plugin_func = ep.load()
                if callable(plugin_func):
                    logger.info(f"Loading plugin from entry point: {ep.name}")
                    plugin_func(self)
                else:
                    logger.warning(f"Plugin {ep.name} does not expose a callable.")
            except Exception as e:
                logger.error(f"Failed to load plugin {ep.name}: {e}")
        
        self._loaded = True

    def register_backend(self, name: str, backend_cls: Type) -> None:
        """Register a new StreamingBackend."""
        self._backends[name] = backend_cls
        logger.debug(f"Registered backend: {name}")

    def register_state_store(self, name: str, state_store_cls: Type) -> None:
        """Register a new StateStore."""
        self._state_stores[name] = state_store_cls
        logger.debug(f"Registered state store: {name}")

    def get_backend(self, name: str) -> Optional[Type]:
        """Retrieve a registered StreamingBackend class."""
        self.load_plugins()
        return self._backends.get(name)

    def get_state_store(self, name: str) -> Optional[Type]:
        """Retrieve a registered StateStore class."""
        self.load_plugins()
        return self._state_stores.get(name)

# Global singleton
plugin_manager = PluginManager()
