import hashlib
import json
from sqlalchemy import create_engine, Column, Integer, String, Text, Numeric, DateTime, JSON, Index
from sqlalchemy.orm import declarative_base # Modern import
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker

# --- 1. Database Configuration (Phase 1: Simple) ---
DATABASE_URL = "postgresql://unlisted_stocks_db_user:x54Z0xvo14vtdUMaclGLrj96jObfZWPw@dpg-d40jksvdiees739l1p30-a/unlisted_stocks_db"

# Boilerplate for SQLAlchemy
Base = declarative_base() # Modern way
engine = create_engine("postgresql://unlisted_stocks_db_user:x54Z0xvo14vtdUMaclGLrj96jObfZWPw@dpg-d40jksvdiees739l1p30-a/unlisted_stocks_db")
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- 2. The Database Model (Blueprint Schema) ---
class UnlistedStock(Base):
    __tablename__ = "unlisted_stocks"

    id = Column(Integer, primary_key=True, index=True)
    
    # Basic Information
    company_name = Column(Text, nullable=False, index=True) 
    symbol = Column(String)
    country = Column(String)
    sector = Column(String)
    status = Column(String)
    
    # Financial Data
    last_known_price = Column(Numeric)
    price_currency = Column(String)
    valuation = Column(String)
    
    # Metadata and Contact
    contact_info = Column(JSON) 
    
    # FIX 1: Renamed Python attribute, kept DB column name
    additional_metadata = Column("metadata", JSON) 
    
    # FIX 2: Changed ARRAY(Text) to String for SQLite compatibility
    tags = Column(String) 
    
    # Source Attribution and Auditing
    source_url = Column(Text, nullable=False)
    source_name = Column(Text, nullable=False)
    retrieved_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)
    
    # Deduplication
    unique_hash = Column(Text, nullable=False, unique=True, index=True)

    __table_args__ = (
        Index('idx_sector_country', 'sector', 'country'),
    )

# --- 3. Deduplication Logic (Data Processing Rules) ---
def create_unique_hash(company_name: str, source_url: str) -> str:
    """
    Creates a SHA256 hash to uniquely identify an entry based on 
    company name and source URL.
    """
    hash_string = f"{company_name.strip().lower()}||{source_url.strip().lower()}"
    hash_bytes = hash_string.encode('utf-8')
    sha26_hash = hashlib.sha256(hash_bytes)
    return sha26_hash.hexdigest()

# --- 4. Function to create the database ---
def create_db_and_tables():
    """
    One-time function to create all tables in the database.
    """
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    # If you run this file directly (python models.py), 
    # it will create the database and table.
    print("Creating database and tables...")
    create_db_and_tables()
    print("Database and tables created successfully with 'unlisted_stocks.db' file.")