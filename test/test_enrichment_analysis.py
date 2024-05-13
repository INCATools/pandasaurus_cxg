import os

import pandas as pd
import pytest

from pandasaurus_cxg.anndata_analyzer import AnndataAnalyzer
from pandasaurus_cxg.anndata_enricher import AnndataEnricher
from pandasaurus_cxg.enrichment_analysis import AnndataEnrichmentAnalyzer


@pytest.fixture()
def sample_file_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "data/immune_example.h5ad")


@pytest.fixture()
def enrichment_analyzer_instance(sample_file_path):
    return AnndataEnrichmentAnalyzer(sample_file_path, author_cell_type_list=["author_cell_type"])


def test_init(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance

    assert isinstance(ae.enricher_manager, AnndataEnricher)
    assert isinstance(ae.analyzer_manager, AnndataAnalyzer)


def test_simple_enrichment(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance

    assert isinstance(ae.simple_enrichment(), pd.DataFrame)


def test_minimal_slim_enrichment(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance

    assert isinstance(ae.minimal_slim_enrichment(["blood_and_immune_upper_slim"]), pd.DataFrame)


def test_full_slim_enrichment(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance

    assert isinstance(ae.full_slim_enrichment(["blood_and_immune_upper_slim"]), pd.DataFrame)


def test_contextual_slim_enrichment(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance

    assert isinstance(ae.contextual_slim_enrichment(), pd.DataFrame)


def test_filter_anndata_with_enriched_cell_type(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance
    ae.simple_enrichment()

    assert isinstance(
        ae.filter_anndata_with_enriched_cell_type(cell_type="CL:0000798"), pd.DataFrame
    )


def test_annotate_anndata_with_cell_type(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance
    ae.simple_enrichment()

    assert isinstance(
        ae.annotate_anndata_with_cell_type(
            cell_type_list=["CL:0000798", "CL:0000815"],
            field_name="new_anno",
            field_value="X'd cells",
        ),
        pd.DataFrame,
    )


def test_co_annotation_report(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance
    ae.simple_enrichment()

    assert isinstance(ae.co_annotation_report(), pd.DataFrame)


def test_enriched_co_annotation_report(enrichment_analyzer_instance):
    ae = enrichment_analyzer_instance
    ae.simple_enrichment()

    assert isinstance(ae.enriched_co_annotation_report(), pd.DataFrame)
