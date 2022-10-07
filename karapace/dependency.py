from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from karapace.schema_models import ValidatedTypedSchema


class Dependency:
    def __init__(self, name: str, subject: str, version: int, schema: "ValidatedTypedSchema") -> None:
        self.name = name
        self.subject = subject
        self.version = version
        self.schema = schema

    def get_schema(self) -> "ValidatedTypedSchema":
        return self.schema

    def identifier(self) -> str:
        return self.name + "_" + self.subject + "_" + str(self.version)


class DependencyVerifierResult:
    def __init__(self, result: bool, message: Optional[str] = ""):
        self.result = result
        self.message = message
