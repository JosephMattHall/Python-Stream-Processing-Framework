from unittest.mock import patch, MagicMock
from typing import Any
from pspf.plugins import PluginManager

class MockBackend:
    pass

class MockStateStore:
    pass

def mock_plugin_entry(manager: PluginManager) -> None:
    manager.register_backend('mock_backend', MockBackend)
    manager.register_state_store('mock_store', MockStateStore)

def test_plugin_registration_and_retrieval() -> None:
    manager = PluginManager()
    
    # Manually register
    manager.register_backend('manual_backend', MockBackend)
    assert manager.get_backend('manual_backend') is MockBackend

@patch('pspf.plugins.importlib.metadata.entry_points')
def test_plugin_auto_discovery(mock_entry_points: MagicMock) -> None:
    """Test that PluginManager can discover and load plugins via entry_points."""
    mock_ep = MagicMock()
    mock_ep.name = "test_plugin"
    mock_ep.load.return_value = mock_plugin_entry
    
    # Handle variations in Python's importlib.metadata.entry_points behaviour
    def mock_ep_func(group: str = "") -> Any:
        if group == 'pspf.plugins':
            return [mock_ep]
        return {'pspf.plugins': [mock_ep]}
        
    mock_entry_points.side_effect = mock_ep_func
    
    manager = PluginManager()
    
    backend = manager.get_backend('mock_backend')
    store = manager.get_state_store('mock_store')
    
    assert backend is MockBackend
    assert store is MockStateStore
    assert mock_ep.load.called
