import os

import anndata
import pandas as pd
import pytest
from pandasaurus.slim_manager import SlimManager

from pandasaurus_cxg.anndata_enricher import AnndataEnricher
from pandasaurus_cxg.utils.exceptions import (
    CellTypeNotFoundError,
    InvalidSlimName,
    MissingEnrichmentProcess,
    SubclassWarning,
)


@pytest.fixture
def sample_immune_data():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "data/immune_example.h5ad")
    return anndata.read(file_path, backed="r")


@pytest.fixture
def slim_data():
    return [
        {
            "name": "blood_and_immune_upper_slim",
            "description": "a subset of general classes related to blood and the immune system, primarily of hematopoietic origin",
        },
        {
            "name": "eye_upper_slim",
            "description": "a subset of general classes related to specific cell types in the eye.",
        },
        {
            "name": "general_cell_types_upper_slim",
            "description": "a subset of general classes of cell types in the cell ontology.",
        },
    ]


def test_init_defaults(mocker, sample_immune_data, slim_data):
    iterable_slim_data = slim_data
    mocker.patch.object(
        SlimManager,
        "get_slim_list",
        side_effect=[
            iter(iterable_slim_data),
        ],
    )
    # Test initialization with default values
    enricher = AnndataEnricher(sample_immune_data)

    assert len(enricher.anndata.obs) == len(sample_immune_data.obs)
    assert len(enricher.anndata.var) == len(sample_immune_data.var)
    assert enricher.anndata.obs["cell_type_ontology_term_id"].equals(
        sample_immune_data.obs["cell_type_ontology_term_id"]
    )
    assert enricher.seed_list == [
        "CL:0000788",
        "CL:0000787",
        "CL:0000798",
        "CL:0000980",
        "CL:0000815",
        "CL:0000897",
        "CL:0000909",
        "CL:0000900",
        "CL:0000895",
        "CL:0000940",
        "CL:0000813",
        "CL:0000809",
        "CL:0002489",
        "CL:0000084",
    ]
    assert enricher._context_list == {"UBERON:0000178": "blood"}
    assert enricher.slim_list == slim_data


def test_init_custom_fields(sample_immune_data):
    # Test initialization with custom field names by swapping 'cell_type' and 'tissue' fields.
    enricher = AnndataEnricher(
        sample_immune_data,
        cell_type_field="tissue_ontology_term_id",
        context_field="cell_type_ontology_term_id",
        context_field_label="cell_type",
    )

    assert enricher.seed_list == ["UBERON:0000178"]
    assert enricher._context_list == {
        "CL:0000788": "naive B cell",
        "CL:0000787": "memory B cell",
        "CL:0000798": "gamma-delta T cell",
        "CL:0000980": "plasmablast",
        "CL:0000815": "regulatory T cell",
        "CL:0000897": "CD4-positive, alpha-beta memory T cell",
        "CL:0000909": "CD8-positive, alpha-beta memory T cell",
        "CL:0000900": "naive thymus-derived CD8-positive, alpha-beta T cell",
        "CL:0000895": "naive thymus-derived CD4-positive, alpha-beta T cell",
        "CL:0000940": "mucosal invariant T cell",
        "CL:0000813": "memory T cell",
        "CL:0000809": "double-positive, alpha-beta thymocyte",
        "CL:0002489": "double negative thymocyte",
        "CL:0000084": "T cell",
    }


def test_init_custom_ontology_list(mocker, sample_immune_data, slim_data):
    iterable_slim_data = slim_data
    mocker.patch.object(
        SlimManager,
        "get_slim_list",
        side_effect=[
            iter(iterable_slim_data),
            iter([{"name": "placeholder_upper_slim", "description": "a placeholder description."}]),
        ],
    )

    # Test initialization with custom ontology list
    custom_ontology_list = ["Cell Ontology", "Uber-anatomy ontology"]
    enricher = AnndataEnricher(sample_immune_data, ontology_list_for_slims=custom_ontology_list)

    iterable_slim_data.append(
        {"name": "placeholder_upper_slim", "description": "a placeholder description."}
    )

    assert enricher.slim_list == iterable_slim_data


def test_init_no_context_field(sample_immune_data):
    # Test initialization when context_field is not present in AnnData
    with pytest.raises(KeyError) as exc_info:
        AnndataEnricher(
            sample_immune_data,
            context_field="nonexistent_field",
            context_field_label="nonexistent_field_label",
        )

    exception = exc_info.value
    assert isinstance(exception, KeyError)

    expected_message = (
        "Please use a valid 'context_field' and 'context_field_label' "
        "that exist in your anndata file."
    )
    assert exception.args[0] == expected_message


def test_from_file_path(mocker, slim_data):
    anndata_file_path = "test/data/human_kidney.h5ad"
    iterable_slim_data = slim_data
    mocker.patch.object(
        SlimManager,
        "get_slim_list",
        side_effect=[
            iter(iterable_slim_data),
            iter(iterable_slim_data),
        ],
    )

    custom_ontology_list = ["Cell Ontology"]
    enricher = AnndataEnricher.from_file_path(
        anndata_file_path, ontology_list_for_slims=custom_ontology_list
    )

    assert isinstance(enricher, AnndataEnricher)
    assert enricher.slim_list == slim_data

    enricher = AnndataEnricher.from_file_path(anndata_file_path)

    assert isinstance(enricher, AnndataEnricher)
    assert enricher.slim_list == slim_data


def test_simple_enrichment(mocker, sample_immune_data):
    enricher = AnndataEnricher(sample_immune_data)
    mocker.patch.object(enricher.enricher, "simple_enrichment", return_value=pd.DataFrame())

    result = enricher.simple_enrichment()

    assert result.empty
    assert isinstance(result, pd.DataFrame)
    enricher.enricher.simple_enrichment.assert_called_once()


def test_minimal_slim_enrichment(mocker, sample_immune_data, slim_data):
    iterable_slim_data = slim_data
    mocker.patch.object(
        SlimManager,
        "get_slim_list",
        side_effect=[
            iter(iterable_slim_data),
        ],
    )

    enricher = AnndataEnricher(sample_immune_data)
    mocker.patch.object(enricher.enricher, "minimal_slim_enrichment", return_value=pd.DataFrame())

    # Test with valid slim_list
    result = enricher.minimal_slim_enrichment(["blood_and_immune_upper_slim"])
    assert result.empty
    assert isinstance(result, pd.DataFrame)
    enricher.enricher.minimal_slim_enrichment.assert_called_once()

    # Test with invalid slim_list
    with pytest.raises(InvalidSlimName) as exc_info:
        enricher.minimal_slim_enrichment(["blood_and_immune_lower_slim"])

    exception = exc_info.value
    expected_message = (
        "The following slim names are invalid: blood_and_immune_lower_slim. "
        "Please use slims from: blood_and_immune_upper_slim, eye_upper_slim, "
        "general_cell_types_upper_slim."
    )

    assert isinstance(exception, InvalidSlimName)
    assert exception.args[0] == expected_message


def test_full_slim_enrichment(mocker, sample_immune_data, slim_data):
    iterable_slim_data = slim_data
    mocker.patch.object(
        SlimManager,
        "get_slim_list",
        side_effect=[
            iter(iterable_slim_data),
        ],
    )

    enricher = AnndataEnricher(sample_immune_data)
    mocker.patch.object(enricher.enricher, "full_slim_enrichment", return_value=pd.DataFrame())

    # Test with valid slim_list
    result = enricher.full_slim_enrichment(["blood_and_immune_upper_slim"])
    assert result.empty
    assert isinstance(result, pd.DataFrame)
    enricher.enricher.full_slim_enrichment.assert_called_once()

    # Test with invalid slim_list
    with pytest.raises(InvalidSlimName) as exc_info:
        enricher.full_slim_enrichment(["blood_and_immune_lower_slim"])

    exception = exc_info.value
    expected_message = (
        "The following slim names are invalid: blood_and_immune_lower_slim. "
        "Please use slims from: blood_and_immune_upper_slim, eye_upper_slim, "
        "general_cell_types_upper_slim."
    )

    assert isinstance(exception, InvalidSlimName)
    assert exception.args[0] == expected_message


def test_contextual_slim_enrichment(mocker, sample_immune_data):
    enricher = AnndataEnricher(
        sample_immune_data,
        context_field="tissue_ontology_term_id",
        context_field_label="tissue",
    )

    mocker.patch.object(
        enricher.enricher, "contextual_slim_enrichment", return_value=pd.DataFrame()
    )

    # Test with context
    result = enricher.contextual_slim_enrichment()
    assert result.empty
    assert isinstance(result, pd.DataFrame)
    enricher.enricher.contextual_slim_enrichment.assert_called_once()


def test_filter_anndata_with_enriched_cell_type(sample_immune_data):
    enricher = AnndataEnricher(sample_immune_data)

    with pytest.raises(MissingEnrichmentProcess) as exc_info:
        enricher.filter_anndata_with_enriched_cell_type("")

    exception = exc_info.value
    expected_message = (
        "Any of the following enrichment methods from AnndataEnricher must be used first; "
        "contextual_slim_enrichment, full_slim_enrichment, minimal_slim_enrichment, "
        "simple_enrichment"
    )

    assert isinstance(exception, MissingEnrichmentProcess)
    assert exception.args[0] == expected_message

    enricher.simple_enrichment()
    with pytest.raises(CellTypeNotFoundError) as exc_info:
        enricher.filter_anndata_with_enriched_cell_type("CL:XXX")

    exception = exc_info.value
    expected_message = (
        "Following cell types not found in the annotation: CL:XXX. Please use cell types from: "
        "CL:0000798, CL:0000809, CL:0000813, CL:0000815, CL:0000895, CL:0000897, CL:0000900, "
        "CL:0000909, CL:0000940, CL:0002489, CL:0000084."
    )

    assert isinstance(exception, CellTypeNotFoundError)
    assert exception.args[0] == expected_message

    cell_type = "CL:0000798"
    filtered_data = enricher.filter_anndata_with_enriched_cell_type(cell_type)
    assert isinstance(filtered_data, pd.DataFrame)
    assert len(filtered_data) > 0
    assert (filtered_data["cell_type_ontology_term_id"] == cell_type).any()


def test_annotate_anndata_with_cell_type(sample_immune_data):
    enricher = AnndataEnricher(sample_immune_data)

    with pytest.raises(MissingEnrichmentProcess) as exc_info:
        enricher.annotate_anndata_with_cell_type(
            cell_type_list=["CL:XXX"], field_name="new_anno", field_value="X'd cells"
        )

    exception = exc_info.value
    expected_message = (
        "Any of the following enrichment methods from AnndataEnricher must be used first; "
        "contextual_slim_enrichment, full_slim_enrichment, minimal_slim_enrichment, "
        "simple_enrichment"
    )

    assert isinstance(exception, MissingEnrichmentProcess)
    assert exception.args[0] == expected_message

    enricher.simple_enrichment()
    with pytest.raises(CellTypeNotFoundError) as exc_info:
        enricher.annotate_anndata_with_cell_type(
            cell_type_list=["CL:XXX"], field_name="new_anno", field_value="X'd cells"
        )

    exception = exc_info.value
    expected_message = (
        "Following cell types not found in the annotation: CL:XXX. Please use cell types from: "
        "CL:0000798, CL:0000809, CL:0000813, CL:0000815, CL:0000895, CL:0000897, CL:0000900, "
        "CL:0000909, CL:0000940, CL:0002489, CL:0000084."
    )

    assert isinstance(exception, CellTypeNotFoundError)
    assert exception.args[0] == expected_message

    enricher.simple_enrichment()
    with pytest.raises(SubclassWarning) as exc_info:
        enricher.annotate_anndata_with_cell_type(
            cell_type_list=["CL:0000809", "CL:0000813", "CL:0000084"],
            field_name="new_anno",
            field_value="X'd cells",
        )

    exception = exc_info.value
    expected_message = (
        "The following cell type terms are related with subClassOf relation. "
        "CL:0000809-CL:0000084, CL:0000813-CL:0000084."
    )

    assert isinstance(exception, SubclassWarning)
    assert exception.args[0] == expected_message

    cell_type_list = ["CL:0000809", "CL:0000813"]
    field_name = "new_anno"
    field_value = "new label"
    enricher.annotate_anndata_with_cell_type(
        cell_type_list=cell_type_list,
        field_name=field_name,
        field_value=field_value,
    )

    # Check if the field is updated as expected for the specified cell types
    condition = enricher.anndata.obs["cell_type_ontology_term_id"].isin(cell_type_list)
    assert (enricher.anndata.obs.loc[condition, field_name] == field_value).all()

    # Check if other observations remain unchanged
    other_condition = ~enricher.anndata.obs["cell_type_ontology_term_id"].isin(cell_type_list)
    assert (enricher.anndata.obs.loc[other_condition, field_name] == "").all()


def test_set_enricher_property_list(sample_immune_data):
    enricher = AnndataEnricher(sample_immune_data)

    assert enricher.enricher._enrichment_property_list == ["rdfs:subClassOf"]

    enricher.set_enricher_property_list(["rdfs:subClassOf", "BFO:0000050"])
    assert enricher.enricher._enrichment_property_list == ["rdfs:subClassOf", "BFO:0000050"]


def test_validate_slim_list(mocker, sample_immune_data, slim_data):
    iterable_slim_data = slim_data
    mocker.patch.object(
        SlimManager,
        "get_slim_list",
        side_effect=[
            iter(iterable_slim_data),
        ],
    )
    valid_slim_list = [
        "blood_and_immune_upper_slim",
        "eye_upper_slim",
        "general_cell_types_upper_slim",
    ]
    enricher = AnndataEnricher(sample_immune_data)
    enricher.validate_slim_list(valid_slim_list)

    invalid_slim_list = ["blood_upper_slim", "eye_lower_slim"]

    with pytest.raises(InvalidSlimName) as exc_info:
        enricher.validate_slim_list(invalid_slim_list)

    exception = exc_info.value
    expected_message = (
        "The following slim names are invalid: blood_upper_slim, eye_lower_slim. "
        "Please use slims from: blood_and_immune_upper_slim, eye_upper_slim, "
        "general_cell_types_upper_slim."
    )

    assert isinstance(exception, InvalidSlimName)
    assert exception.args[0] == expected_message


def test_create_cell_type_dict(sample_immune_data):
    data = {
        "s": ["cell_type_1", "cell_type_2", "cell_type_3"],
        "s_label": ["Label 1", "Label 2", "Label 3"],
        "o": ["cell_type_4", "cell_type_5", "cell_type_6"],
        "o_label": ["Label 4", "Label 5", "Label 6"],
    }
    enricher = AnndataEnricher(sample_immune_data)
    enricher.enricher.enriched_df = pd.DataFrame(data)

    # Call the create_cell_type_dict method
    cell_type_dict = enricher.create_cell_type_dict()

    # Define the expected dictionary based on the sample data
    expected_dict = {
        "cell_type_1": "Label 1",
        "cell_type_2": "Label 2",
        "cell_type_3": "Label 3",
        "cell_type_4": "Label 4",
        "cell_type_5": "Label 5",
        "cell_type_6": "Label 6",
    }

    # Check if the returned dictionary matches the expected dictionary
    assert cell_type_dict == expected_dict


def test_check_subclass_relationships(sample_immune_data):
    enricher = AnndataEnricher(sample_immune_data)
    with pytest.raises(MissingEnrichmentProcess) as exc_info:
        enricher.check_subclass_relationships([""])

    exception = exc_info.value
    expected_message = (
        "Any of the following enrichment methods from AnndataEnricher must be used first; "
        "contextual_slim_enrichment, full_slim_enrichment, minimal_slim_enrichment, "
        "simple_enrichment"
    )

    assert isinstance(exception, MissingEnrichmentProcess)
    assert exception.args[0] == expected_message

    enricher.simple_enrichment()
    subclass_relation = enricher.check_subclass_relationships(
        ["CL:0000897", "CL:0000813", "CL:0000909", "CL:0000809"]
    )

    expected_subclass_relation = [("CL:0000897", "CL:0000813"), ("CL:0000909", "CL:0000813")]

    assert subclass_relation == expected_subclass_relation
