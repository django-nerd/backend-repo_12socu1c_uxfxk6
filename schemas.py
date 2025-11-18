"""
Database Schemas

Define your MongoDB collection schemas here using Pydantic models.
These schemas are used for data validation in your application.

Each Pydantic model represents a collection in your database.
Model name is converted to lowercase for the collection name:
- User -> "user" collection
- Product -> "product" collection
- BlogPost -> "blogs" collection
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# Example schemas (you can keep these for reference or remove later)
class User(BaseModel):
    """
    Users collection schema
    Collection name: "user" (lowercase of class name)
    """
    name: str = Field(..., description="Full name")
    email: str = Field(..., description="Email address")
    address: str = Field(..., description="Address")
    age: Optional[int] = Field(None, ge=0, le=120, description="Age in years")
    is_active: bool = Field(True, description="Whether user is active")

class Product(BaseModel):
    """
    Products collection schema
    Collection name: "product" (lowercase of class name)
    """
    title: str = Field(..., description="Product title")
    description: Optional[str] = Field(None, description="Product description")
    price: float = Field(..., ge=0, description="Price in dollars")
    category: str = Field(..., description="Product category")
    in_stock: bool = Field(True, description="Whether product is in stock")

# App-specific schemas for scraped content
class TableData(BaseModel):
    headers: List[str]
    rows: List[List[str]]

class ScrapePage(BaseModel):
    """
    Stores parsed content from a scraped page.
    Collection name: "scrapepage"
    """
    url: str
    path: Optional[str] = None
    title: Optional[str] = None
    tables: List[TableData] = []
    scraped_at: Optional[datetime] = None
    extra: Optional[Dict[str, Any]] = None

# Conversions
class ConversionRecord(BaseModel):
    """
    Normalized currency/resource conversion rate extracted from pages.
    Collection name: "conversion"
    """
    page_url: str
    page_title: Optional[str] = None
    source: str = Field(..., description="Source currency/resource name, e.g., Gem")
    target: str = Field(..., description="Target currency/resource name, e.g., Coin")
    rate: float = Field(..., gt=0, description="How many target units per 1 source unit")
    text: Optional[str] = Field(None, description="Original text snippet the rate was parsed from")
    context: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class ConversionsUpsert(BaseModel):
    page_url: str
    page_title: Optional[str] = None
    items: List[ConversionRecord]

class ExtractRequest(BaseModel):
    url: Optional[str] = None
    id: Optional[str] = None
    ocr: bool = False
    """If true, attempt to parse information from image metadata/alt text as a lightweight OCR surrogate."""
