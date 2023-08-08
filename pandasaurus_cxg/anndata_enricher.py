import itertools
from typing import List, Optional

import pandas as pd
from anndata import AnnData
from pandasaurus.query import Query
from pandasaurus.slim_manager import SlimManager

from pandasaurus_cxg.anndata_loader import AnndataLoader
from pandasaurus_cxg.utils.exceptions import (
    CellTypeNotFoundError,
    InvalidSlimName,
    SubclassWarning,
)


class AnndataEnricher:
    """Enriches anndata object with functional annotations using various enrichment methods."""

    # TODO think a better approach for cell_type_field and context_field selection/passing
    def __init__(
        self,
        anndata: AnnData,
        cell_type_field: Optional[str] = "cell_type_ontology_term_id",
        context_field: Optional[str] = "tissue_ontology_term_id",
        ontology_list_for_slims: Optional[List[str]] = None,
    ):
        """Initialize the AnndataEnricher instance with AnnData object.

        Args:

            anndata: The AnnData object
            cell_type_field: The cell type information in the anndata object.
                Defaults to "cell_type_ontology_term_id".
            context_field: The context information in the anndata object.
                Defaults to "tissue_ontology_term_id".
            ontology_list_for_slims: The ontology list for generating the slim list.
                The slim list is used in minimal_slim_enrichment and full_slim_enrichment.
                Defaults to "Cell Ontology"
        """
        if ontology_list_for_slims is None:
            ontology_list_for_slims = ["Cell Ontology"]
        # TODO Do we need to keep whole anndata? Would it be enough to keep the obs only?
        self._anndata = anndata
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

    @staticmethod
    def from_file_path(
        file_path: str,
        cell_type_field: Optional[str] = "cell_type_ontology_term_id",
        context_field: Optional[str] = "tissue_ontology_term_id",
        ontology_list_for_slims: Optional[List[str]] = None,
    ):
        """Initialize the AnndataEnricher instance with file path.

        Args:

            file_path: The path to the file containing the anndata object.
            cell_type_field: The cell type information in the anndata object.
                Defaults to "cell_type_ontology_term_id".
            context_field: The context information in the anndata object.
                Defaults to "tissue_ontology_term_id".
            ontology_list_for_slims: The ontology list for generating the slim list.
                The slim list is used in minimal_slim_enrichment and full_slim_enrichment.
                Defaults to "Cell Ontology"
        """
        if ontology_list_for_slims is None:
            ontology_list_for_slims = ["Cell Ontology"]
        return AnndataEnricher(
            AnndataLoader.load_from_file(file_path),
            cell_type_field,
            context_field,
            ontology_list_for_slims,
        )

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
        # TODO Add empty dataframe exception
        cell_type_dict = self.create_cell_type_dict()
        if cell_type not in cell_type_dict:
            raise CellTypeNotFoundError(cell_type, cell_type_dict.keys())

        cell_type_group = self.enriched_df[self.enriched_df["o"] == cell_type]["s"].tolist()
        cell_type_group.append(cell_type)

        return self._anndata.obs[
            self._anndata.obs["cell_type_ontology_term_id"].isin(cell_type_group)
        ]

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
        # TODO Add empty dataframe exception
        cell_type_dict = self.create_cell_type_dict()
        # Check if any cell_type in cell_type_list is not in cell_type_dict
        missing_cell_types = set(cell_type_list) - set(cell_type_dict.keys())
        if missing_cell_types:
            raise CellTypeNotFoundError(missing_cell_types, list(cell_type_dict.keys()))

        # preprocess
        subclass_relation = self.check_subclass_relationships(cell_type_list)
        if subclass_relation:
            raise SubclassWarning(subclass_relation)

        # annotation phase
        self._anndata.obs[field_name] = ""
        condition = self._anndata.obs["cell_type_ontology_term_id"].isin(cell_type_list)
        self._anndata.obs.loc[condition, field_name] = field_value
        return self._anndata.obs[self._anndata.obs[field_name] == field_value]

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

    def get_seed_list(self):
        return self.__seed_list

    def get_anndata(self):
        return self._anndata

    def create_cell_type_dict(self):
        # TODO Add empty dataframe exception
        return pd.concat(
            [
                self.enriched_df[["s", "s_label"]],
                self.enriched_df[["o", "o_label"]].rename(columns={"o": "s", "o_label": "s_label"}),
            ],
            axis=0,
            ignore_index=True,
        ).drop_duplicates().set_index("s")["s_label"].to_dict()

    def check_subclass_relationships(self, cell_type_list: List[str]):
        # TODO Add empty dataframe exception
        subclass_relation = []
        for s, o in itertools.combinations(cell_type_list, 2):
            if not self.enriched_df[
                (self.enriched_df["s"] == s)
                & (self.enriched_df["p"] == "rdfs:subClassOf")
                & (self.enriched_df["o"] == o)
            ].empty:
                subclass_relation.append([s, o])

            if not self.enriched_df[
                (self.enriched_df["s"] == o)
                & (self.enriched_df["p"] == "rdfs:subClassOf")
                & (self.enriched_df["o"] == s)
            ].empty:
                subclass_relation.append([o, s])
        return subclass_relation
