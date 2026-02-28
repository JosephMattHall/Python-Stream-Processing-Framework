# Plugin System

PSPF features a robust plugin ecosystem, allowing developers to extend the framework with custom Streaming Backends and State Stores without modifying the core codebase.

## Overview

Plugins are discovered using Python's standard `importlib.metadata` entry points. The framework looks for registration functions in the `pspf.plugins` entry-point group.

## Using Plugins

To use a plugin, simply refer to its registered name when configuring your backend or state store.

```python
from pspf.plugins import plugin_manager

# Get a custom backend registered by a plugin
kafka_backend = plugin_manager.get_backend("kafka", **config)

# Get a custom state store
custom_store = plugin_manager.get_state_store("my-redis-store", **config)
```

## Developing a Plugin

To create a PSPF plugin:

1.  **Implement the Interface**: Your backend must inherit from `StreamingBackend` or your state store from `StateStore`.
2.  **Define a Registration Function**: This function should register your implementation with the `plugin_manager`.

```python
# my_plugin.py
from pspf.plugins import plugin_manager

def register():
    plugin_manager.register_backend("my-custom-backend", MyCustomBackend)
```

3.  **Register Entry Point**: Add the entry point in your plugin's `pyproject.toml` or `setup.py`.

**pyproject.toml (Poetry Example):**
```toml
[tool.poetry.plugins."pspf.plugins"]
"my_custom" = "my_plugin:register"
```

## Built-in Plugins
PSPF includes several built-in plugins by default:
- **Backends**: `valkey`, `memory`, `file`.
- **State Stores**: `sqlite`, `memory`, `rocksdb`.
