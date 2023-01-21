import sqlalchemy
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Float,Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
import uuid
from sqlalchemy.sql import func


Base  = declarative_base()

class Face(Base):
    __tablename__ = 'face'
    __table_args__ = (
        {
            "postgresql_partition_by": "LIST (entity)",
        },
    )
    id  = Column( postgresql.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    redisPartitionKey = Column(String, nullable=True)
    processIdentifier = Column(String)
    embedding = Column(postgresql.ARRAY(sqlalchemy.Float))
    processInfo = Column( postgresql.JSON(), nullable=True)
    updateInfo = Column( postgresql.JSON(), nullable=False)
    embedding_size=Column(Integer)
    status = Column(String)
    images = Column(postgresql.ARRAY(postgresql.BYTEA))
    createTS = Column(DateTime(timezone=True), server_default=func.now())
    entity = Column(String,primary_key=True)
    category_name = Column(String)

class Category(Base):
    __tablename__ = 'category'
    name = Column(String, primary_key=True)
    entity = Column(String, primary_key=True)
    model =Column(String)
    embeddingLength=Column(Integer)
    Info = Column( postgresql.JSON(), nullable=True)
    updateInfo = Column( postgresql.JSON(), nullable=False)



