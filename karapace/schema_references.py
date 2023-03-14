from karapace.typing import JsonData, ResolvedVersion, Subject
from typing import Any, List

Referents = List


class Reference:
    def __init__(self, name: str, subject: Subject, version: ResolvedVersion):
        self.name = name
        self.subject = subject
        self.version = version

    def identifier(self) -> str:
        return self.name + "_" + self.subject + "_" + str(self.version)

    def to_dict(self) -> JsonData:
        return {
            "name": self.name,
            "subject": self.subject,
            "version": self.version,
        }

    def __repr__(self) -> str:
        return f"{{name='{self.name}', subject='{self.subject}', version={self.version}}}"

    def __hash__(self) -> int:
        return hash((self.name, self.subject, self.version))

    def __eq__(self, other: Any) -> bool:
        if other is None or not isinstance(other, Reference):
            return False
        return self.name == other.name and self.subject == other.subject and self.version == other.version
