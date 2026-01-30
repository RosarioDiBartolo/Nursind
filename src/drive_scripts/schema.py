from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from .names import normalize_name


# -----------------------------
# Pydantic models (replacement for TypedDicts)
# -----------------------------

class Outputs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    days_csv: Optional[str] = None
    pairs_csv: Optional[str] = None
    totals_json: Optional[str] = None
    report_json: Optional[str] = None


class IndexFile(BaseModel):
    model_config = ConfigDict(extra="forbid")
    employee: str
    employee_id: Optional[str] = None

    file_id: Optional[str] = None
    file_name: Optional[str] = None
    outputs: Optional[Outputs] = None
    reason: Optional[str] = None
    type: Optional[Literal["file", "folder"]] = None
 


 

 

# -----------------------------
# Replacement for the dataclass
# -----------------------------

class EmployeeAggregate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    employee: str
    employee_id: Optional[str] = None
    included: List[IndexFile] = Field(default_factory=list)
    excluded: List[IndexFile] = Field(default_factory=list)


# -----------------------------
# Helper
# -----------------------------

def employee_key(employee: Optional[str], employee_id: Optional[str]) -> str:
    if employee_id:
        return f"id:{employee_id}"
    return f"name:{normalize_name(employee)}"
