from pydantic import BaseModel, Field, constr, conint, conlist,ConfigDict
from datetime import datetime, timedelta
from typing import List,Optional
from bson import ObjectId






#----------------------Shop collection models----------------------
class ShopModel(BaseModel):
    name: str
    domain: str
    shop_urls: List[str]
    last_scanned: datetime = Field(default_factory=datetime.utcnow)  # Default to current UTC time
    products_on_sale_percentage: int = Field(default=0)  # Default to 0% if not provided
    scan_frequency: str = Field(default="72 hours")  # Default to "72 hours" if not provided
    next_scan_due_date: datetime = Field(default_factory=datetime.utcnow)  # Default to current UTC time if not provided

    class Config:
        # Allow MongoDB to handle datetime formatting
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }
class ShopUpdateModel(BaseModel):
    last_scanned: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of the last scan")
    products_on_sale_percentage: conint(ge=0, le=100) = Field(..., description="Percentage of products on sale")
    scan_frequency: str = Field(..., pattern=r'^\d+ (hours|days)$', description="Frequency of scans, e.g., '72 hours'")
    next_scan_due_date: datetime = Field(..., description="Timestamp for the next scheduled scan")






#----------------------Products collection models----------------------
class ProductModel(BaseModel):
    shop_id: ObjectId = Field(..., description="ID of the shop this product belongs to")
    title: str = Field(..., description="Title of the product")
    title2: str = Field(..., description="Title of the product inside of the variants object")
    price: float
    compare_at_price: Optional[float] = None
    is_on_sale: Optional[bool] = False
    last_scanned: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of the last scan")

    model_config = ConfigDict(arbitrary_types_allowed=True)
class UpdateProductModel(BaseModel):
    price: Optional[float] = Field(None, description="Updated price of the variant")
    compare_at_price: Optional[float] = Field(None, description="Updated compare_at price of the variant")
    last_scanned: Optional[datetime] = Field(default_factory=datetime.utcnow, description="Updated timestamp of the last scan")
    is_on_sale: Optional[bool] = False
    model_config = ConfigDict(arbitrary_types_allowed=True)






#----------------------Price_history collection models----------------------
class PriceHistoryModel(BaseModel):
    product_id: ObjectId = Field(..., description="ID of the product this history snapshot belongs to")
    price: float
    compare_at_price: Optional[float] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Timestampt of when this item was scanned")
    model_config = ConfigDict(arbitrary_types_allowed=True)
class UpdatePriceHistoryModel(BaseModel):
    price: float
    compare_at_price: Optional[float] = None
    timestmap: datetime = Field(default_factory=datetime.utcnow, description="Timestampt of when this item was scanned")
    model_config = ConfigDict(arbitrary_types_allowed=True)
