from typing import Optional

from pandasaurus_cxg.anndata_analyzer import AnndataAnalyzer
from pandasaurus_cxg.anndata_enricher import AnndataEnricher
from pandasaurus_cxg.utils.anndata_loader import AnndataLoader


class AnndataEnrichmentAnalyzer:
    def __init__(self, file_path: str, author_cell_type_list: Optional[str] = None):
        """
        Initializes the AnndataEnrichmentAnalyzer, a wrapper for AnndataEnricher and AnndataAnalyzer.

        Args:
            file_path (str): The path to the file containing the anndata object.
            author_cell_type_list (Optional[str]): Names of optional free text cell type fields.
                If the 'obs_meta' field is missing in 'anndata.uns', this parameter should be set.
                This is used to define free text cell type fields.
        """
        anndata = AnndataLoader.load_from_file(file_path)
        self.enricher_manager = AnndataEnricher(anndata)
        self.analyzer_manager = AnndataAnalyzer(anndata, author_cell_type_list)
