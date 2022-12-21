from typing import Any, Callable, Optional

from orjson import orjson
from pydantic import BaseModel


def orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


class Model(BaseModel):
    """
    To find all the configuration options for a model, just visit the link below
    https://pydantic-docs.helpmanual.io/usage/model_config/
    """

    class Config:
        # To activate an ORM service for our models
        orm_mode = False
        # Allows us to rename the field-names to conform to a defined standard
        alias_generator: Optional[Callable]
        # To strip a leading or trailing whitespace
        anystr_strip_whitespace = True
        # Whether to use the enum key or value
        use_enum_values = False
        # Perform validation on assignment
        validate_assignment = True
        allow_population_by_field_name = True
        # Checks if the value is an instance of the type
        arbitrary_types_allowed = True

        json_loads: Callable[[str], Any] = orjson.loads
        json_dumps: Callable[..., str] = orjson_dumps

    # @overload
    # @classmethod
    # def parse_file(cls, ):
