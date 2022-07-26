from typing import Any, Dict, Union

JsonData = Any  # Data that will be encoded to or has been parsed from JSON

Subject = str

# Container type for a subject, with configuration settings and all the schemas
SubjectData = Dict[str, Any]

Version = Union[int, str]
ResolvedVersion = int
