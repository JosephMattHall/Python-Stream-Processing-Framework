# Schema Evolution in PSPF

As your application grows, your data models (events) will inevitably change. PSPF uses Pydantic for schema validation, which provides powerful tools for handling schema evolution safely.

## Core Principles

1.  **Backward Compatibility**: New versions of a consumer should be able to read old messages in the stream.
2.  **Forward Compatibility**: Old versions of a consumer (if still running) should not crash when they encounter a new message format.
3.  **Valkey is Schemaless**: Valkey stores your data as JSON. The "schema" is enforced by your Python code, not the database.

---

## Common Scenarios

### 1. Adding a New Field (Safe)
Adding a field is usually safe if you provide a **default value**.

```python
# Old Schema
class UserSignup(BaseEvent):
    user_id: str

# New Schema (Safe)
class UserSignup(BaseEvent):
    user_id: str
    referral_code: Optional[str] = None  # Old messages will have None
```

### 2. Modifying a Field (Caution)
If you change a field type (e.g., `int` to `str`), the consumer will fail validation when it encounters an old message.

**Solution**: Use Pydantic's `@field_validator` or `AliasPath` to handle both old and new formats.

```python
from pydantic import field_validator

class MyEvent(BaseEvent):
    price: float
    
    @field_validator('price', mode='before')
    @classmethod
    def handle_old_int(cls, v):
        if isinstance(v, int):
            return float(v)
        return v
```

### 3. Renaming a Field (Caution)
Renaming a field will break consumers that expect the old name.

**Solution**: Use `Field(validation_alias=...)` to accept both names.

```python
class UserSignup(BaseEvent):
    # Consumer will accept 'uid' (old) or 'user_id' (new)
    user_id: str = Field(validation_alias=AliasChoices('user_id', 'uid'))
```

---

## Best Practices

- **Never remove fields**: Instead, mark them as deprecated and eventually `Optional`.
- **Use Optional for new fields**: Until you are sure all producers are sending the new field.
- **Version your Steam Keys**: If you are making a massive, breaking change, it is often safer to create a new stream (e.g., `user_signups_v2`) and migrate.
- **Test with Old Data**: Always run your new consumer against a sample of old data from your production or staging stream before deploying.

---

## Handling Validation Errors
If a message fails validation, PSPF will:
1. Log a `ValidationError`.
2. Move the message to the **Dead Letter Office (DLO)** after max retries.
3. Allow you to inspect the raw JSON in the DLO to understand why it failed.

You can then use `pspfctl replay` after fixing your schema to bring those messages back into the main stream.
