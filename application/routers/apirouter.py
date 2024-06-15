from fastapi import APIRouter
from typing import Optional
from pydantic import BaseModel
from ..services.databaseService import ExecuteQuery

class DbConnection(BaseModel):
    engine: str
    host: str
    port: str
    db: str
    query: str
    user: Optional[str] = None
    password: Optional[str] = None

router = APIRouter(prefix="/database-api/v1")

@router.post("/execute", status_code=200)
def execute(config: DbConnection):
    print("Consuming: " + config.engine)
    return ExecuteQuery.execute(config)