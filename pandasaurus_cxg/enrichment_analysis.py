from typing import List, Optional

import pandas as pd

from pandasaurus_cxg.anndata_analyzer import AnndataAnalyzer
from pandasaurus_cxg.anndata_enricher import AnndataEnricher
from pandasaurus_cxg.utils.anndata_loader import AnndataLoader


class AnndataEnrichmentAnalyzer:
    def __init__(self, file_path: str, author_cell_type_list: Optional[List[str]] = None):
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

    def simple_enrichment(self) -> pd.DataFrame:
        """Perform simple enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame.
        """
        return self.enricher_manager.simple_enrichment()

    def minimal_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        """Perform minimal slim enrichment analysis.

        Args:
            slim_list (List[str]): The list of slim terms to use for enrichment analysis.

        Returns:
           The enriched results as a pandas DataFrame.
        """
        return self.enricher_manager.minimal_slim_enrichment(slim_list)

    def full_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        """Perform full slim enrichment analysis.

        Args:
            slim_list (List[str]): The list of slim terms to use for enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame.
        """
        return self.enricher_manager.full_slim_enrichment(slim_list)

    def contextual_slim_enrichment(self) -> Optional[pd.DataFrame]:
        """Perform contextual slim enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame if the context list is available,
                otherwise None.
        """
        # TODO Better handle datasets without tissue field
        return self.enricher_manager.contextual_slim_enrichment()

    def filter_anndata_with_enriched_cell_type(self, cell_type: str) -> pd.DataFrame:
        """Filter the original anndata object based on enriched cell types.

        Args:
            cell_type: CURIE of the cell type for filtering.

        Returns:
            pd.DataFrame: A DataFrame containing the filtered observations from the original
                anndata object.

        Raises:
            CellTypeNotFoundError: If the provided cell_type is not found in the enriched cell types.

        """
        return self.enricher_manager.filter_anndata_with_enriched_cell_type(cell_type)

    def annotate_anndata_with_cell_type(
        self, cell_type_list: List[str], field_name: str, field_value: str
    ) -> pd.DataFrame:
        """Annotates the AnnData object with cell type information.

        This method annotates the cells in the AnnData object with specific cell type
        information based on the provided `cell_type_list`. It sets the value of the specified
        `field_name` to the given `field_value` for the cells whose 'cell_type_ontology_term_id'
        matches any of the values in `cell_type_list`.

        Args:
            cell_type_list (List[str]): A list of cell type ontology term IDs to be used for
                cell type annotation.
            field_name (str): The name of the field/column in the AnnData object where the
                cell type information will be stored.
            field_value (str): The value to be assigned to cells with matching cell type
                ontology term IDs in the specified `field_name`.

        Returns:
            pd.DataFrame: A DataFrame containing the updated observations from the original anndata
                object.

        Raises:
            CellTypeNotFoundError: If any cell type in `cell_type_list` is not found in the
                available cell types in the dataset.
            SubclassWarning: If the provided `cell_type_list` contains any relationships where
                one cell type is a subclass of another, indicating a potential issue with the
                provided annotations.
        """
        return self.enricher_manager.annotate_anndata_with_cell_type(
            cell_type_list, field_name, field_value
        )

    def co_annotation_report(self, disease: Optional[str] = None, enrich: bool = False):
        """
        Generates a co-annotation report based on the provided schema.

        Args:
            disease (Optional[str]): A valid disease CURIE used to filter the rows based on the
                given disease. If provided, only the rows matching the specified disease will be
                included in the filtering process. Defaults to None if no disease filtering is
                desired.
            enrich (bool): Flag to either enable or disable enrichment in co_annotation report.
                Defaults to False.

        Returns:
            pd.DataFrame: The co-annotation report.

        """
        return self.analyzer_manager.co_annotation_report(disease, enrich)

    def enriched_co_annotation_report(self, disease: Optional[str] = None):
        """
        Generates an enriched co-annotation report based on the provided schema. The enrichment
        process will be performed by checking if any of the CL terms in the initial seed
        (the set of CL terms used to initialize the Pandasaurus object) are also present in the
        object column of the enrichment table. If a match is found, the co-annotation analysis
        will be repeated, including everything that maps to this term, either directly or via
        the enrichment table.

        Args:
            disease (Optional[str]): A valid disease CURIE used to filter the rows based on the
                given disease. If provided, only the rows matching the specified disease will be
                included in the filtering process. Defaults to None if no disease filtering is
                desired.

        Returns:
            pd.DataFrame: The co-annotation report.

        """
        return self.analyzer_manager.enriched_co_annotation_report(disease)
