import pytest

from pandasaurus_cxg.utils.anndata_loader import AnndataLoader


def test_load_from_file_failure(tmp_path):
    # Create a non-existent file path
    file_path = "non_existent_file.h5ad"

    # Attempt to load an AnnData object from a non-existent file
    loaded_anndata = AnndataLoader.load_from_file(file_path)

    # Check that the loaded object is None (indicating failure)
    assert loaded_anndata is None
