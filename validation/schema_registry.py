from dataclasses import dataclass
from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)


# ── Data classes (unchanged — rest of validation layer depends on these) ──

@dataclass
class Column:
    name:           str
    dtype:          str
    nullable:       bool
    allowed_values: list[str] = None


@dataclass
class Schema:
    """Expectation for a single source file/table."""
    file:        str
    primary_key: str
    columns:     list[Column]

    def __init__(self, file: str, primary_key: str, columns: list[Column]):
        self.file        = file
        self.primary_key = primary_key
        self.columns     = columns

    def get_column(self, column_name: str) -> Column | None:
        return next((col for col in self.columns if col.name == column_name), None)

    def get_required_columns(self) -> list[str]:
        return [col.name for col in self.columns if not col.nullable]

    def get_categorical_columns(self) -> list[Column]:
        return [col for col in self.columns if col.allowed_values is not None]


# ── Registry ─────────────────────────────────────────────────────────────

class schema_registry:
    """
    Builds Schema objects on demand from pipeline_config.yaml -> schemas section.

    The YAML structure expected:
        schemas:
          customers:
            primary_key: customer_id
            columns:
              - { name: customer_id, dtype: int, nullable: false }
              - { name: full_name,   dtype: str, nullable: true  }
              - ...

    No hardcoded schemas here — add or modify schemas in pipeline_config.yaml only.
    """

    def __init__(self):
        cfg = get_config()
        self._raw: dict = cfg.get("schemas", {})

        if not self._raw:
            logger.error(
                "No 'schemas' section found in pipeline_config.yaml. "
                "All schema lookups will return None."
            )

    # ------------------------------------------------------------------ #

    def get_schema(self, file: str) -> Schema | None:
        """
        Build and return a Schema object for the given file name.
        Returns None (and logs an error) if the file has no entry in config.

        Parameters
        ----------
        file : str
            File name without extension, e.g. "customers", "orders"
        """
        raw_schema = self._raw.get(file)

        if raw_schema is None:
            logger.error(f"No schema defined for '{file}' in pipeline_config.yaml")
            return None

        try:
            columns = [
                Column(
                    name           = col["name"],
                    dtype          = col["dtype"],
                    nullable       = col["nullable"],
                    allowed_values = col.get("allowed_values"),   # optional key
                )
                for col in raw_schema["columns"]
            ]

            return Schema(
                file        = file,
                primary_key = raw_schema["primary_key"],
                columns     = columns,
            )

        except KeyError as e:
            logger.error(
                f"Malformed schema for '{file}' in pipeline_config.yaml — "
                f"missing key: {e}"
            )
            return None