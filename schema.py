from pydantic import BaseModel, Json
from typing import Optional, List, Any
from enum import Enum
from uuid import uuid4, UUID

class addCategory(BaseModel):
    name: str
    model: str
    embeddingLength:int
    username: Optional[str]
    class Config:
        orm_mode = True

class addFace(BaseModel):
    id: Optional[UUID]
    category: str
    embedding : List[float]
    embedding_size : int
    model : str
    processIdentifier : str
    processInfo : Optional[Any]
    images: List[str]
    realtime: bool
    class Config:
        orm_mode = True

class addFaceTempRT(BaseModel):
    id: Optional[UUID]
    embedding : List[float]
    embedding_size : int
    model : str
    processIdentifier : str
    class Config:
        orm_mode = True

class deleteFace(BaseModel):
    id: Optional[UUID]
    embedding_size : int
    model : str
    processIdentifier : str

class findface(BaseModel):
    category: str
    embedding: List[float]
    maxFaces: int

class findfaceResult(BaseModel):
    matched: List[str]
    unmatched: List[str]
