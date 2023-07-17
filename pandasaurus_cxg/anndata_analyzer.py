from enum import Enum
import itertools
import os
from typing import List, Optional

import pandas as pd
from pandas import DataFrame

from pandasaurus_cxg.anndata_enricher import AnndataEnricher
from pandasaurus_cxg.anndata_loader import AnndataLoader
from pandasaurus_cxg.schema.schema_loader import read_json_file


# Check if the DEBUG environment variable is set
debug_mode = os.getenv("DEBUG")


class AnndataAnalyzer:
    """
    A class for providing methods for different type of analysis in an AnnData object.

    Args:
        file_path (str): The path to the AnnData file.
        schema_path (str): The path to the schema file.

    Attributes:
        _anndata (DataFrame): The observation data from the AnnData object.
        _schema (dict): The schema data loaded from the schema file.

    """

    def __init__(self, file_path: str, schema_path: str):
        """
        Initializes an instance of the AnndataAnalyzer class.

        Args:
            file_path (str): The path to the AnnData file.
            schema_path (str): The path to the schema file.

        """
        self.file_path = file_path
        self._anndata = AnndataLoader.load_from_file(file_path)
        self._schema = read_json_file(schema_path)

    def co_annotation_report(self, disease: Optional[str] = None, enrich: bool = False):
        """
        Generates a co-annotation report based on the provided schema.

        Args:
            disease (Optional[str]): A valid disease CURIE used to filter the rows based on the
                given disease. If provided, only the rows matching the specified disease will be
                included in the filtering process. Defaults to None if no disease filtering is
                desired.
            enrich: Flag to either enable or disable enrichment in co_annotation report.
                Defaults to False.

        Returns:
            DataFrame: The co-annotation report.

        """
        free_text_cell_type = [key for key, value in self._schema.items() if value]
        temp_result = []
        cell_type_combinations = list(itertools.combinations(free_text_cell_type, 2))
        for combination in cell_type_combinations:
            field_name_1 = combination[1]
            field_name_2 = combination[0]
            if (
                field_name_1 in self._anndata.obs.columns
                and field_name_2 in self._anndata.obs.columns
            ):
                co_oc = self._filter_data_and_remove_duplicates(field_name_1, field_name_2, disease)

                if enrich:
                    co_oc = self._enrich_co_annotation(co_oc, field_name_1, field_name_2)

                AnndataAnalyzer._assign_predicate_column(co_oc, field_name_1, field_name_2)
                temp_result.extend(co_oc.to_dict(orient="records"))

        result = [
            [item for sublist in [[k, v] for k, v in record.items()] for item in sublist]
            for record in temp_result
        ]
        unique_result = AnndataAnalyzer._remove_duplicates(result)
        return DataFrame(
            [inner_list[:2] + inner_list[5:6] + inner_list[2:4] for inner_list in unique_result],
            columns=["field_name1", "value1", "predicate", "field_name2", "value2"],
        )

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
            DataFrame: The co-annotation report.

        """
        return self.co_annotation_report(disease, True)

    def _enrich_co_annotation(self, co_oc, field_name_1, field_name_2):
        enricher = AnndataEnricher(self._anndata)
        simple = enricher.simple_enrichment()
        df = simple[simple["o"].isin(enricher.get_seed_list())][["s_label", "o_label"]].rename(
            columns={"s_label": field_name_1, "o_label": field_name_2}
        )
        co_oc = pd.concat([co_oc, df], axis=0).reset_index(drop=True)
        return co_oc

    def _filter_data_and_remove_duplicates(self, field_name_1, field_name_2, disease):
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
    def _remove_duplicates(data: List[List[str]]) -> List[List[str]]:
        unique_data = []
        unique_set = set()

        for sublist in data:
            if Predicate.SUPERCLUSTER_OF.value in sublist:
                continue
            sorted_sublist = tuple(sorted(set(sublist)))
            if sorted_sublist not in unique_set:
                unique_data.append(sublist)
                unique_set.add(sorted_sublist)
        return unique_data

    @staticmethod
    def _assign_predicate_column(co_oc, field_name_1, field_name_2):
        # Group by field_name_2 and field_name_1 to create dictionaries
        field_name_2_dict = co_oc.groupby(field_name_2)[field_name_1].apply(list).to_dict()
        field_name_1_dict = co_oc.groupby(field_name_1)[field_name_2].apply(list).to_dict()
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
