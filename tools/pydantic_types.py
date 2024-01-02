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
    prodName: str = None
    prodCatid: Optional[int] = None
    prodCat: Optional[str] = None
    prodPcatid: Optional[int] = None
    prodPcat: Optional[str] = None
    lowPrice: float = None
    highPrice: float = None
    avgPrice: float = None
    place: str = None
    specInfo: Optional[str] = None
    unitInfo: Optional[str] = None
    pubDate: datetime = None
    status: Optional[Any] = None


class XinFaDiPriceDetailResponse(BaseModel):
    current: int
    limit: int
    count: int
    list: List[XinFaDiPriceDetailResponseDataObject]
