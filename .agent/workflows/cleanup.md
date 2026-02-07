---
description: Clean up legacy files and directories
---

This workflow removes legacy code from the previous architecture that is no longer compatible with the new Enterprise features.

1. Remove legacy runtime and operators
// turbo
2. rm -rf pspf/runtime pspf/operators

3. Remove legacy utilities and models
// turbo
4. rm pspf/utils/metrics.py pspf/utils/tracing.py pspf/models.py

5. Remove legacy connectors
// turbo
6. rm pspf/connectors/base.py pspf/connectors/kafka.py pspf/connectors/mqtt.py pspf/connectors/log_source.py pspf/connectors/dead_letter.py
