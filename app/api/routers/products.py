from __future__ import annotations

from fastapi import APIRouter, status

from app.api.deps import ProductServiceDependency
from app.api.schemas.product import ProductCreate, ProductResponse

router = APIRouter(prefix="/products", tags=["products"])


@router.post("", response_model=ProductResponse, status_code=status.HTTP_201_CREATED)
async def create_product(
    payload: ProductCreate,
    products: ProductServiceDependency,
) -> ProductResponse:
    product = await products.create_product(
        name=payload.name,
        price=payload.price,
        stock=payload.stock,
    )
    return ProductResponse.model_validate(product)


@router.get("", response_model=list[ProductResponse])
async def list_products(products: ProductServiceDependency) -> list[ProductResponse]:
    return [ProductResponse.model_validate(product) for product in await products.list_products()]
