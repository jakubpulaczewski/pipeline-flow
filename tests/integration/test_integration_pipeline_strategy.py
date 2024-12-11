# Standard Imports

# Third-party Imports
import pytest

# Project Imports
from core.pipeline_strategy import (
    ETLStrategy, 
    ELTStrategy, 
    ETLTStrategy
)

@pytest.mark.asyncio
async def test_etl_strategy(etl_pipeline_factory) -> None:
    etl_pipeline = etl_pipeline_factory(name="Job1")

    result = await ETLStrategy().execute(etl_pipeline)

    assert result == True


@pytest.mark.asyncio
async def test_elt_strategy(elt_pipeline_factory) -> None:
    elt_pipeline = elt_pipeline_factory(name="Job1")
    
    result = await ELTStrategy().execute(elt_pipeline)

    assert result == True


@pytest.mark.asyncio
async def test_etlt_strategy(etlt_pipeline_factory) -> None:
    etlt_pipeline = etlt_pipeline_factory(name="Job1")

    result = await ETLTStrategy().execute(etlt_pipeline)
    assert result == True
