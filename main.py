from pydantic import BaseModel
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware # To allow frontend to connect
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import decimal # For handling NUMERIC type

# --- 1. Import from our models.py file ---
from models import UnlistedStock, SessionLocal, Base, engine, create_db_and_tables

# --- 2. Create Pydantic Models (API Response Shape) ---
# This defines what the JSON response will look like.
# It ensures our API sends clean, validated data.

class StockResponse(BaseModel):
    id: int
    company_name: str
    symbol: Optional[str] = None
    country: Optional[str] = None
    sector: Optional[str] = None
    status: Optional[str] = None
    last_known_price: Optional[decimal.Decimal] = None
    price_currency: Optional[str] = None
    valuation: Optional[str] = None
    source_name: str
    source_url: str
    retrieved_at: datetime
    
    class Config:
        from_attributes = True # <-- This is the modern V2 standard

class StockResponse(BaseModel):
    id: int
    company_name: str
    symbol: Optional[str] = None
    country: Optional[str] = None
    sector: Optional[str] = None
    status: Optional[str] = None
    last_known_price: Optional[decimal.Decimal] = None
    price_currency: Optional[str] = None
    valuation: Optional[str] = None
    source_name: str
    source_url: str
    retrieved_at: datetime
    
    # This tells Pydantic to read data even if it's
    # a database object (not just a dict)
    class Config:
        orm_mode = True

# --- 3. Initialize the FastAPI App ---
app = FastAPI(
    title="Unlisted Stocks API",
    description="API for unlisted stock data scraped from public sources.",
    version="1.0.0"
)

@app.on_event("startup")
def on_startup():
    print("Running startup event: creating database tables...")
    create_db_and_tables()
    print("Database tables created.")

# --- 4. Add CORS Middleware ---
# This is CRITICAL. It allows your React frontend (on a different "origin")
# to make requests to this backend API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # For development, allow all. Change to frontend URL later.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 5. Database Dependency ---
# This is a "dependency" function.
# It creates a new database session for each API request
# and closes it when the request is done.
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- 6. API Endpoints (from Blueprint) ---

@app.get("/")
def read_root():
    """ A simple root endpoint to check if the API is running. """
    return {"message": "Welcome to the Unlisted Stocks API!"}


# --- NEW: Main Filterable Endpoint ---
@app.get("/api/unlisted", response_model=List[StockResponse])
def get_listings(
    db: Session = Depends(get_db),
    sector: Optional[str] = None,
    country: Optional[str] = None,
    limit: int = 50
):
    """
    Fetches a paginated list of all entries, with optional filtering.
    Corresponds to: GET /api/unlisted
    And advanced filtering: GET /api/unlisted?sector=Finance&country=India
    
    """
    
    # Start with a base query
    query = db.query(UnlistedStock)
    
    # Apply filters if they are provided
    if sector:
        query = query.filter(UnlistedStock.sector == sector)
        
    if country:
        query = query.filter(UnlistedStock.country == country)
    
    # Add ordering and limit
    listings = (
        query.order_by(UnlistedStock.retrieved_at.desc())
        .limit(limit)
        .all()
    )
    
    return listings


# --- UPDATED: /api/latest is now just an alias ---
@app.get("/api/latest", response_model=List[StockResponse])
def get_latest_listings(db: Session = Depends(get_db)):
    """
    Fetches the 10 most recent listings.
    Corresponds to: GET /api/latest
    [cite: 104]
    """
    # This just calls our main function with a limit of 10
    return get_listings(db=db, limit=10)


# --- 7. Search Endpoint (No changes needed) ---

@app.get("/api/search", response_model=List[StockResponse])
def search_listings(q: str, db: Session = Depends(get_db)):
    """
    Searches listings by company name.
    Corresponds to: GET /api/search?q=...
    [cite: 105]
    """
    if not q:
        return []
        
    search_term = f"%{q}%" 
    
    search_results = (
        db.query(UnlistedStock)
        .filter(UnlistedStock.company_name.ilike(search_term))
        .order_by(UnlistedStock.retrieved_at.desc())
        .limit(25)
        .all()
    )
    
    return search_results