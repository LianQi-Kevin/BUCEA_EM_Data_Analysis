from datetime import datetime
from typing import List, Optional, Any

from pydantic import BaseModel, Field


class XinFaDiPriceDetailRequest(BaseModel):
    limit: int = 20
    current: int = None
    pubDateStartTime: str = None
    pubDateEndTime: str = None
    prodPcatid: int = None
    prodCatid: int = None
    prodName: str = None


class XinFaDiPriceDetailResponseDataObject(BaseModel):
    object_id: int = Field(..., alias="id")
    prodName: str
    prodCatid: Optional[Any] = None
    prodCat: Optional[Any] = None
    prodPcatid: Optional[Any] = None
    prodPcat: Optional[str] = None
    lowPrice: float
    highPrice: float
    avgPrice: float
    place: Optional[Any] = None
    specInfo: Optional[Any] = None
    unitInfo: Optional[Any] = None
    pubDate: datetime = None
    status: Optional[Any] = None


class XinFaDiPriceDetailResponse(BaseModel):
    current: int
    limit: int
    count: int
    list: List[XinFaDiPriceDetailResponseDataObject]
