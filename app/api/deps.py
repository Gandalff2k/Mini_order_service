from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.infra.db import get_session
from app.repositories.product_repository import ProductRepository
from app.services.product_service import ProductService

SessionDependency = Annotated[AsyncSession, Depends(get_session)]


def get_product_service(session: SessionDependency) -> ProductService:
    return ProductService(session, ProductRepository(session))


ProductServiceDependency = Annotated[ProductService, Depends(get_product_service)]
