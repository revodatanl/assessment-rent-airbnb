"""Sanity checks for DLT pipeline definition (L3)."""

from pathlib import Path


def test_dlt_pipeline_file_exists_and_declares_tables():
    path = Path(__file__).parent.parent / "dlt" / "rent_airbnb_pipeline.py"
    source = path.read_text()
    assert "import dlt" in source
    assert "@dlt.table" in source
    assert "@dlt.expect_or_drop" in source
    assert "gold_investment_comparison" in source
    assert "gold_top_opportunities" in source
