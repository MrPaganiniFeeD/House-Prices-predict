from sqlalchemy import Column, Integer, JSON, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class TrainingData(Base):
    __tablename__ = "training_data"

    id = Column(Integer, primary_key=True, index=True)
    features = Column(JSON, nullable=False)
    target = Column(Integer, nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<TrainingData(id={self.id}, target={self.target})>"


class TestData(Base):
    __tablename__ = 'test_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    features = Column(JSON, nullable=False) 
    original_id = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<TestData(id={self.id}, original_id={self.original_id})>"

__all__ = ['Base']