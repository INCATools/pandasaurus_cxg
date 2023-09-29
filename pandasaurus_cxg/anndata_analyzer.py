import json
import os
from enum import Enum
from typing import List, Optional

import pandas as pd
from anndata import AnnData

from pandasaurus_cxg.anndata_enricher import AnndataEnricher
from pandasaurus_cxg.schema.cell_x_gene_schema import required_fields
from pandasaurus_cxg.utils.anndata_loader import AnndataLoader

# Check if the DEBUG environment variable is set
debug_mode = os.getenv("DEBUG")


class AnndataAnalyzer:
    """
    A class for providing methods for different type of analysis in an AnnData object.

    Args:
        anndata (AnnData): The AnnData object.
        author_cell_type_list (Optional[List[str]]): Names of optional free text cell type fields.
            If the 'obs_meta' field is missing in 'anndata.uns', this parameter should be set.
                This is used to define free text cell type fields.

    Attributes:
        _anndata (pd.DataFrame): The observation data from the AnnData object.
        all_cell_type_identifiers (List[str]): All available cell type identifiers.

    """

    def __init__(self, anndata: AnnData, author_cell_type_list: Optional[List[str]] = None):
        """
        Initializes the AnndataAnalyzer instance with AnnData object.

        Args:
            anndata (AnnData): The AnnData object.
            author_cell_type_list (Optional[List[str]]): Names of optional free text cell type fields.
                If the 'obs_meta' field is missing in 'anndata.uns', this parameter should be set.
                This is used to define free text cell type fields.

        Raises:
            ValueError: If the 'obs_meta' field is missing in anndata.uns and author_cell_type_list is not provided.
                This indicates that the necessary information about cell types is not available.
        """
        self._anndata = anndata
        try:
            obs_meta = json.loads(anndata.uns["obs_meta"])
            self.all_cell_type_identifiers = [
                meta.get("field_name")
                for meta in obs_meta
                if meta.get("field_type") == "author_cell_type_label"
            ] + ["cell_type"]
        except KeyError:
            if author_cell_type_list:
                self.all_cell_type_identifiers = author_cell_type_list + ["cell_type"]
                self._anndata.uns["obs_meta"] = json.dumps(
                    [
                        [{"field_name": item, "field_type": "author_cell_type_label"}]
                        for item in author_cell_type_list
                    ]
                )
                # TODO do we need to save this?
            else:
                available_free_text_fields = sorted(
                    list(set(self._anndata.obs.columns) - set(required_fields))
                )
                raise ValueError(
                    "AnndataAnalyzer initialization error:\n\n"
                    "The 'obs_meta' field is missing in anndata.uns!\n"
                    "If this field is absent, you can provide a list of field names from the "
                    "AnnData file using the author_cell_type_list parameter.\n"
                    f"Available author cell type fields are: {', '.join(available_free_text_fields)}"
                )
        self.report_df = pd.DataFrame()

    @staticmethod
    def from_file_path(file_path: str, author_cell_type_list: Optional[List[str]] = None):
        """
        Initializes the AnndataAnalyzer instance with file path.

        Args:
            file_path (str): The path to the AnnData file.
            author_cell_type_list (Optional[List[str]]): Names of optional free text cell type fields.
                If the 'obs_meta' field is missing in 'anndata.uns', this parameter should be set.
                This is used to define free text cell type fields.

        """
        return AnndataAnalyzer(AnndataLoader.load_from_file(file_path), author_cell_type_list)

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
        # TODO needs a refactoring about what enrichment method to use. Or would it better to accept
        #  enriched_df as parameter, so users get to decide?
        enriched_co_oc = None
        if enrich:
            enricher = AnndataEnricher(self._anndata)
            enricher.simple_enrichment()
            enriched_co_oc = AnndataAnalyzer._enrich_co_annotation(enricher)
        temp_result = []
        for field_name_2 in self.all_cell_type_identifiers:
            for field_name_1 in self.all_cell_type_identifiers:
                if (
                    field_name_1 != field_name_2
                    and field_name_1 in self._anndata.obs.columns
                    and field_name_2 in self._anndata.obs.columns
                ):
                    co_oc = self._filter_data_and_drop_duplicates(
                        field_name_1, field_name_2, disease
                    )

                    if enrich:
                        co_oc = pd.concat(
                            [
                                co_oc,
                                enriched_co_oc.rename(
                                    columns={"s_label": field_name_1, "o_label": field_name_2}
                                ),
                            ],
                            axis=0,
                        ).reset_index(drop=True)

                    AnndataAnalyzer._assign_predicate_column(co_oc, field_name_1, field_name_2)
                    temp_result.extend(co_oc.to_dict(orient="records"))

        result = [
            [item for sublist in [[k, v] for k, v in record.items()] for item in sublist]
            for record in temp_result
        ]
        unique_result = AnndataAnalyzer._remove_duplicates(result)
        self.report_df = pd.DataFrame(
            [inner_list[:2] + inner_list[5:6] + inner_list[2:4] for inner_list in unique_result],
            columns=["field_name1", "value1", "predicate", "field_name2", "value2"],
        )
        return self.report_df

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
        return self.co_annotation_report(disease, True)

    @staticmethod
    def _enrich_co_annotation(enricher: AnndataEnricher):
        enriched_df = enricher.enricher.enriched_df
        if enriched_df.empty:
            return pd.DataFrame()
        return enriched_df[enriched_df["o"].isin(enricher.seed_list)][["s_label", "o_label"]]

    def _filter_data_and_drop_duplicates(self, field_name_1, field_name_2, disease):
        # Filter the data based on the disease condition
        co_oc = (
            self._anndata.obs[
                (self._anndata.obs["disease_ontology_term_id"].str.lower() == disease.lower())
            ][[field_name_1, field_name_2]]
            if disease
            else self._anndata.obs[[field_name_1, field_name_2]]
        )
        # Drop duplicates
        co_oc = co_oc.drop_duplicates().reset_index(drop=True)
        return co_oc

    @staticmethod
    def _remove_duplicates(data: List[List[str]]):
        # TODO do a clean up/rename if it is necessary
        # Currently used only to clean up supercluster_of relations
        unique_data = []

        for sublist in data:
            if Predicate.SUPERCLUSTER_OF.value in sublist:
                continue
            unique_data.append(sublist)
        return unique_data

    @staticmethod
    def _assign_predicate_column(co_oc, field_name_1, field_name_2):
        # Group by field_name_2 and field_name_1 to create dictionaries
        field_name_2_dict = (
            co_oc.groupby(field_name_2, observed=True)[field_name_1].apply(list).to_dict()
        )
        field_name_1_dict = (
            co_oc.groupby(field_name_1, observed=True)[field_name_2].apply(list).to_dict()
        )
        # Assign the "predicate" column using self._assign_predicate method
        co_oc["predicate"] = co_oc.apply(
            AnndataAnalyzer._assign_predicate,
            args=(
                field_name_1,
                field_name_2,
                field_name_1_dict,
                field_name_2_dict,
                debug_mode,
            ),
            axis=1,
        )

    @staticmethod
    def _assign_predicate(
        row, field_name_1, field_name_2, field_name_1_dict, field_name_2_dict, debug
    ):
        if debug:
            print("Debugging row:", row)
            print("Value of field_name_1:", row[field_name_1])
            print("Value of field_name_1_dict:", field_name_1_dict.get(row[field_name_1], []))
            print("Value of field_name_2:", row[field_name_2])
            print("Value of field_name_2_dict:", field_name_2_dict.get(row[field_name_2], []))

        field_name_1_values = field_name_1_dict.get(row[field_name_1], [])
        field_name_2_values = field_name_2_dict.get(row[field_name_2], [])

        if field_name_2_dict.get(row[field_name_2], []) == [
            row[field_name_1]
        ] and field_name_1_dict.get(row[field_name_1], []) == [row[field_name_2]]:
            return Predicate.CLUSTER_MATCHES.value

        if (
            row[field_name_1] in field_name_2_values
            and row[field_name_2] in field_name_1_values
            and len(field_name_1_values) == 1
        ):
            return Predicate.SUBCLUSTER_OF.value

        if (
            row[field_name_1] in field_name_2_values
            and row[field_name_2] in field_name_1_values
            and len(field_name_2_values) == 1
        ):
            return Predicate.SUPERCLUSTER_OF.value

        return Predicate.CLUSTER_OVERLAPS.value


class Predicate(Enum):
    CLUSTER_MATCHES = "cluster_matches"
    CLUSTER_OVERLAPS = "cluster_overlaps"
    SUBCLUSTER_OF = "subcluster_of"
    SUPERCLUSTER_OF = "supercluster_of"
