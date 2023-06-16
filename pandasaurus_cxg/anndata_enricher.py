from typing import List, Optional

import pandas as pd
from pandasaurus.query import Query
from pandasaurus.slim_manager import SlimManager

from pandasaurus_cxg.anndata_loader import AnndataLoader
from pandasaurus_cxg.utils.exceptions import InvalidSlimName


class AnndataEnricher:
    """Enriches anndata object with functional annotations using various enrichment methods."""

    # TODO think a better approach for cell_type_field and context_field selection/passing
    def __init__(
        self,
        file_path: str,
        cell_type_field: Optional[str] = "cell_type_ontology_term_id",
        context_field: Optional[str] = "tissue_ontology_term_id",
        ontology_list_for_slims: Optional[List[str]] = ["Cell Ontology"],
    ):
        """Initialize the AnndataEnricher instance.

        Args:
            file_path: The path to the file containing the anndata object.
            cell_type_field: The field name for the cell type information in the anndata object.
                Defaults to "cell_type_ontology_term_id".
            context_field: The field name for the context information in the anndata object.
                Defaults to "tissue_ontology_term_id".
            ontology_list_for_slims: The ontology list for generating the slim list.
                The slim list is used in minimal_slim_enrichment and full_slim_enrichment.
                Defaults to "Cell Ontology"
        """
        # TODO Do we need to keep whole anndata? Would it be enough to keep the obs only?
        self._anndata = AnndataLoader.load_from_file(file_path)
        self.__seed_list = self._anndata.obs[cell_type_field].unique().tolist()
        self.__enricher = Query(self.__seed_list)
        self.__context_list = (
            None
            if context_field not in self._anndata.obs.keys()
            else self._anndata.obs[context_field].unique().tolist()
        )
        self.slim_list = [
            slim
            for ontology in ontology_list_for_slims
            for slim in SlimManager.get_slim_list(ontology)
        ]
        self.enriched_df = pd.DataFrame()

    def simple_enrichment(self) -> pd.DataFrame:
        """Perform simple enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame.
        """
        self.enriched_df = self.__enricher.simple_enrichment()
        return self.enriched_df

    def minimal_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        """Perform minimal slim enrichment analysis.

        Args:
            slim_list (List[str]): The list of slim terms to use for enrichment analysis.

        Returns:
           The enriched results as a pandas DataFrame.
        """
        self.validate_slim_list(slim_list)
        self.enriched_df = self.__enricher.minimal_slim_enrichment(slim_list)
        return self.enriched_df

    def full_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        """Perform full slim enrichment analysis.

        Args:
            slim_list (List[str]): The list of slim terms to use for enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame.
        """
        self.validate_slim_list(slim_list)
        self.enriched_df = self.__enricher.full_slim_enrichment(slim_list)
        return self.enriched_df

    def contextual_slim_enrichment(self) -> Optional[pd.DataFrame]:
        """Perform contextual slim enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame if the context list is available,
                otherwise None.
        """
        # TODO Better handle datasets without tissue field
        self.enriched_df = (
            self.__enricher.contextual_slim_enrichment(self.__context_list)
            if self.__context_list
            else None
        )
        return self.enriched_df

    def filter_anndata_by_enriched_cell_type(self) -> pd.DataFrame:
        """Filter the original anndata object based on enriched cell types.

        Returns:
            The filtered observations from the original anndata object.
        """
        # TODO 's' amd 'o' part should be revised
        cell_type_list = set(self.enriched_df["s"].tolist() + self.enriched_df["o"].tolist())
        return self._anndata.obs[
            self._anndata.obs["cell_type_ontology_term_id"].isin(cell_type_list)
        ]

    def set_enricher_property_list(self, property_list: List[str]):
        """Set the property list for the enricher.

        Args:
            property_list (List[str]): The list of properties to include in the enrichment analysis.
        """
        self.__enricher = Query(self.__seed_list, property_list)

    def validate_slim_list(self, slim_list):
        """Check if any slim term in the given list is invalid.

        Args:
            slim_list (List[str]): The list of slim terms to check.

        Raises:
            InvalidSlimName: If any slim term in the slim_list is invalid.

        """
        invalid_slim_list = [
            item for item in slim_list if item not in [slim.get("name") for slim in self.slim_list]
        ]
        if invalid_slim_list:
            raise InvalidSlimName(invalid_slim_list, self.slim_list)
