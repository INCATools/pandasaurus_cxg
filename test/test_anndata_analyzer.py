import os

import anndata
import pandas as pd
import pytest

from pandasaurus_cxg.anndata_analyzer import AnndataAnalyzer
from pandasaurus_cxg.anndata_enricher import AnndataEnricher


@pytest.fixture
def sample_immune_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "data/immune_example.h5ad")
    return anndata.read(file_path, backed="r")


@pytest.fixture
def sample_anndata_with_uns():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "data/modified_human_kidney.h5ad")
    return anndata.read(file_path, backed="r")


@pytest.fixture
def sample_anndata_without_uns():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "data/human_kidney.h5ad")
    return anndata.read(file_path, backed="r")


@pytest.fixture()
def author_cell_type_list():
    return [
        "subclass.full",
        "subclass.l3",
        "subclass.l2",
        "subclass.l1",
        "class",
        "author_cell_type",
    ]


def test_init_with_uns(sample_anndata_with_uns, author_cell_type_list):
    analyzer = AnndataAnalyzer(sample_anndata_with_uns)
    assert analyzer.all_cell_type_identifiers == author_cell_type_list + ["cell_type"]


def test_init_without_uns(sample_anndata_without_uns):
    with pytest.raises(ValueError) as exc_info:
        AnndataAnalyzer(sample_anndata_without_uns)

    exception = exc_info.value
    expected_message = (
        "AnndataAnalyzer initialization error:\n\n"
        "The 'obs_meta' field is missing in anndata.uns!\n"
        "If this field is absent, you can provide a list of field names from the "
        "AnnData file using the author_cell_type_list parameter.\n"
        "Available author cell type fields are: BMI, G2M.Score, S.Score, aEpi.score, aStr.score, "
        "class, collagen.score, condition.l1, condition.l2, condition.long, cyc.score, degen.score, "
        "diabetes_history, eGFR, experiment, glycoprotein.score, hypertension, id, library, "
        "matrisome.score, nCount_RNA, nFeature_RNA, pagoda_k100_infomap_coembed, percent.cortex, "
        "percent.er, percent.medulla, percent.mt, proteoglycan.score, region.l1, region.l2, "
        "specimen, state, state.l2, structure, subclass.full, subclass.l1, subclass.l2, subclass.l3"
    )

    assert isinstance(exception, ValueError)
    assert exception.args[0] == expected_message


def test_init_with_author_cell_type_list(sample_anndata_without_uns, author_cell_type_list):
    analyzer = AnndataAnalyzer(
        sample_anndata_without_uns, author_cell_type_list=author_cell_type_list
    )
    assert analyzer.all_cell_type_identifiers == author_cell_type_list + ["cell_type"]


def test_from_file_path(author_cell_type_list):
    anndata_file_path = "test/data/human_kidney.h5ad"
    analyzer = AnndataAnalyzer.from_file_path(
        anndata_file_path, author_cell_type_list=author_cell_type_list
    )

    assert isinstance(analyzer, AnndataAnalyzer)


def test_co_annotation_report_without_enrichment(sample_anndata_without_uns, author_cell_type_list):
    analyzer = AnndataAnalyzer(
        sample_anndata_without_uns, author_cell_type_list=author_cell_type_list
    )
    report_df = analyzer.co_annotation_report()

    assert isinstance(report_df, pd.DataFrame)
    assert report_df.shape == (1203, 5)


def test_co_annotation_report_with_enrichment(sample_anndata_without_uns, author_cell_type_list):
    # TODO needs a refactoring after _enrich_co_annotation refactoring
    analyzer = AnndataAnalyzer(
        sample_anndata_without_uns, author_cell_type_list=author_cell_type_list
    )
    report_df = analyzer.co_annotation_report(enrich=True)

    assert isinstance(report_df, pd.DataFrame)
    assert report_df.shape == (1203, 5)


def test_co_annotation_report_with_disease_filter(
    sample_anndata_without_uns, author_cell_type_list
):
    analyzer = AnndataAnalyzer(
        sample_anndata_without_uns, author_cell_type_list=author_cell_type_list
    )
    report_df = analyzer.co_annotation_report(disease="PATO:0000461")
    anndata_obs = analyzer._anndata.obs

    assert isinstance(report_df, pd.DataFrame)
    assert (
        "PATO:0000461"
        in anndata_obs[
            anndata_obs[report_df.iloc[0]["field_name1"]] == report_df.iloc[0]["value1"]
        ]["disease_ontology_term_id"]
        .unique()
        .tolist()
    )


def test_enriched_co_annotation_report(sample_immune_data, author_cell_type_list):
    # TODO needs a refactoring after co_annotation_report refactoring
    analyzer = AnndataAnalyzer(sample_immune_data, author_cell_type_list=author_cell_type_list)
    report_df = analyzer.enriched_co_annotation_report()

    assert isinstance(report_df, pd.DataFrame)
    assert report_df.shape == (30, 5)


def test_enrich_co_annotation(sample_immune_data, author_cell_type_list):
    analyzer = AnndataAnalyzer(sample_immune_data, author_cell_type_list=author_cell_type_list)
    enricher = AnndataEnricher(sample_immune_data)
    result_df = analyzer._enrich_co_annotation(enricher)

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty

    enricher.simple_enrichment()
    result_df = analyzer._enrich_co_annotation(enricher)

    assert isinstance(result_df, pd.DataFrame)
    assert result_df.shape == (6, 2)
