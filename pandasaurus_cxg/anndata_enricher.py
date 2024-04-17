import itertools
from typing import List, Optional, Tuple

import pandas as pd
from anndata import AnnData
from pandasaurus.query import Query
from pandasaurus.slim_manager import SlimManager

from pandasaurus_cxg.utils.anndata_loader import AnndataLoader
from pandasaurus_cxg.utils.exceptions import (
    CellTypeNotFoundError,
    InvalidSlimName,
    MissingEnrichmentProcess,
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
        context_field_label: Optional[str] = "tissue",
        ontology_list_for_slims: Optional[List[str]] = None,
    ):
        """Initialize the AnndataEnricher instance with AnnData object.

        Args:

            anndata: The AnnData object.
            cell_type_field: The cell type information in the anndata object.
                Defaults to "cell_type_ontology_term_id".
            context_field: Ontology ID of the context information in the anndata object.
                Defaults to "tissue_ontology_term_id".
            context_field_label: Label of the context information in the anndata object.
                Defaults to "tissue".
            ontology_list_for_slims: The ontology list for generating the slim list.
                The slim list is used in minimal_slim_enrichment and full_slim_enrichment.
                Defaults to "Cell Ontology"
        """
        if ontology_list_for_slims is None:
            ontology_list_for_slims = ["Cell Ontology"]
        # TODO Do we need to keep whole anndata? Would it be enough to keep the obs only?
        self.anndata = anndata
        self.seed_dict = dict(
            self.anndata.obs.drop_duplicates(subset=[cell_type_field, "cell_type"])[
                [cell_type_field, "cell_type"]
            ].values
        )
        self.enricher = Query(list(self.seed_dict.keys()))
        try:
            unique_context = self.anndata.obs[
                [context_field, context_field_label]
            ].drop_duplicates()
            self._context_list = (
                None
                if context_field not in self.anndata.obs.keys()
                else dict(zip(unique_context[context_field], unique_context[context_field_label]))
            )
        except KeyError as e:
            raise KeyError(
                "Please use a valid 'context_field' and 'context_field_label' that exist in your anndata file."
            )
        self.slim_list = [
            slim
            for ontology in ontology_list_for_slims
            for slim in SlimManager.get_slim_list(ontology)
        ]

    @staticmethod
    def from_file_path(
        file_path: str,
        cell_type_field: Optional[str] = "cell_type_ontology_term_id",
        context_field: Optional[str] = "tissue_ontology_term_id",
        context_field_label: Optional[str] = "tissue",
        ontology_list_for_slims: Optional[List[str]] = None,
    ):
        """Initialize the AnndataEnricher instance with file path.

        Args:

            file_path: The path to the file containing the anndata object.
            cell_type_field: The cell type information in the anndata object.
                Defaults to "cell_type_ontology_term_id".
            context_field: Ontology ID of the context information in the anndata object.
                Defaults to "tissue_ontology_term_id".
            context_field_label: Label of the context information in the anndata object.
                Defaults to "tissue".
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
            context_field_label,
            ontology_list_for_slims,
        )

    def simple_enrichment(self) -> pd.DataFrame:
        """Perform simple enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame.
        """
        return self.enricher.simple_enrichment()

    def minimal_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        """Perform minimal slim enrichment analysis.

        Args:
            slim_list (List[str]): The list of slim terms to use for enrichment analysis.

        Returns:
           The enriched results as a pandas DataFrame.
        """
        self.validate_slim_list(slim_list)
        return self.enricher.minimal_slim_enrichment(slim_list)

    def full_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        """Perform full slim enrichment analysis.

        Args:
            slim_list (List[str]): The list of slim terms to use for enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame.
        """
        self.validate_slim_list(slim_list)
        return self.enricher.full_slim_enrichment(slim_list)

    def contextual_slim_enrichment(self) -> Optional[pd.DataFrame]:
        """Perform contextual slim enrichment analysis.

        Returns:
            The enriched results as a pandas DataFrame if the context list is available,
                otherwise None.
        """
        # TODO Better handle datasets without tissue field
        # TODO self._context_list is refactored and cannot be None in any case. 'else' needs an update
        return (
            self.enricher.contextual_slim_enrichment(list(self._context_list.keys()))
            if self._context_list
            else None
        )

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
        cell_type_dict = self.create_cell_type_dict()
        if cell_type not in cell_type_dict:
            raise CellTypeNotFoundError([cell_type], cell_type_dict.keys())

        cell_type_group = self.enricher.enriched_df[self.enricher.enriched_df["o"] == cell_type][
            "s"
        ].tolist()
        cell_type_group.append(cell_type)

        return self.anndata.obs[
            self.anndata.obs["cell_type_ontology_term_id"].isin(cell_type_group)
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
        cell_type_dict = self.create_cell_type_dict()
        # Check if any cell_type in cell_type_list is not in cell_type_dict
        missing_cell_types = set(cell_type_list) - set(cell_type_dict.keys())
        if missing_cell_types:
            raise CellTypeNotFoundError(list(missing_cell_types), list(cell_type_dict.keys()))

        # preprocess
        subclass_relation = self.check_subclass_relationships(cell_type_list)
        if subclass_relation:
            raise SubclassWarning(subclass_relation)

        # annotation phase
        self.anndata.obs[field_name] = ""
        condition = self.anndata.obs["cell_type_ontology_term_id"].isin(cell_type_list)
        self.anndata.obs.loc[condition, field_name] = field_value
        return self.anndata.obs[self.anndata.obs[field_name] == field_value]

    def set_enricher_property_list(self, property_list: List[str]):
        """Set the property list for the enricher.

        Args:
            property_list (List[str]): The list of properties to include in the enrichment analysis.
        """
        self.enricher = Query(list(self.seed_dict.keys()), property_list)

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

    def create_cell_type_dict(self):
        """
        Create a dictionary from enriched_df for mapping cell type ontology term IDs to their labels.

        Returns:
            Dict[str, str]: A dictionary where keys are cell type ontology term IDs (e.g., "CL:000001") and values
            are corresponding cell type labels (e.g., "Neuron").

        Raises:
            MissingEnrichmentProcess: If the enrichment process has not been performed, and the
                `enriched_df` is empty.
        """
        if self.enricher.enriched_df.empty:
            enrichment_methods = [i for i in dir(AnndataEnricher) if "_enrichment" in i]
            enrichment_methods.sort()
            raise MissingEnrichmentProcess(enrichment_methods)
        return (
            pd.concat(
                [
                    self.enricher.enriched_df[["s", "s_label"]],
                    self.enricher.enriched_df[["o", "o_label"]].rename(
                        columns={"o": "s", "o_label": "s_label"}
                    ),
                ],
                axis=0,
                ignore_index=True,
            )
            .drop_duplicates()
            .set_index("s")["s_label"]
            .to_dict()
        )

    def check_subclass_relationships(self, cell_type_list: List[str]) -> List[Tuple[str, str]]:
        """
        Check for subclass relationships between cell type ontology terms using enriched_df.

        Args:
            cell_type_list: A list of cell type ontology term IDs to be used
                for cell type annotation.

        Returns:
            A list of cell type pairs that have a subClassOf relationship between them.

        Raises:
            MissingEnrichmentProcess: If the enrichment process has not been performed, and the
                `enriched_df` is empty.
        """
        if self.enricher.enriched_df.empty:
            enrichment_methods = [i for i in dir(AnndataEnricher) if "_enrichment" in i]
            enrichment_methods.sort()
            raise MissingEnrichmentProcess(enrichment_methods)
        subclass_relation = []
        for s, o in itertools.combinations(cell_type_list, 2):
            if not self.enricher.enriched_df[
                (self.enricher.enriched_df["s"] == s)
                & (self.enricher.enriched_df["p"] == "rdfs:subClassOf")
                & (self.enricher.enriched_df["o"] == o)
            ].empty:
                subclass_relation.append((s, o))

            if not self.enricher.enriched_df[
                (self.enricher.enriched_df["s"] == o)
                & (self.enricher.enriched_df["p"] == "rdfs:subClassOf")
                & (self.enricher.enriched_df["o"] == s)
            ].empty:
                subclass_relation.append((o, s))
        return subclass_relation
