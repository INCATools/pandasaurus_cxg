from enum import Enum

import pandas as pd

from pandasaurus_cxg.anndata_loader import AnndataLoader
from pandasaurus_cxg.schema.schema_loader import read_json_file


class AnndataAnalyzer:
    """
    A class for providing methods for different type of analysis in an AnnData object.

    Args:
        file_path (str): The path to the AnnData file.
        schema_path (str): The path to the schema file.

    Attributes:
        _anndata_obs (pd.DataFrame): The observation data from the AnnData object.
        _schema (dict): The schema data loaded from the schema file.

    """

    def __init__(self, file_path: str, schema_path: str):
        """
        Initializes an instance of the AnndataAnalyzer class.

        Args:
            file_path (str): The path to the AnnData file.
            schema_path (str): The path to the schema file.

        """
        self._anndata_obs = AnndataLoader.load_from_file(file_path).obs
        self._schema = read_json_file(schema_path)

    def co_annotation_report(self):
        """
        Generates a co-annotation report based on the provided schema.

         Examples:
            | subclass.l3, dPT, cluster_matches, subclass.full, Degenerative Proximal Tubule Epithelial Cell
            | subclass.l3, aTAL1, subcluster_of, subclass.full, Adaptive / Maladaptive / Repairing Thick Ascending Limb Cell
            | class, epithelial cells, cluster_matches, cell_type, kidney collecting duct intercalated cell

        Returns:
            pd.DataFrame: The co-annotation report.

        """
        free_text_cell_type = [key for key, value in self._schema.items() if value]
        temp_result = []
        for field_name_2 in free_text_cell_type:
            for field_name_1 in free_text_cell_type:
                if (
                    field_name_1 != field_name_2
                    and field_name_1 in self._anndata_obs.columns
                    and field_name_2 in self._anndata_obs.columns
                ):
                    co_oc = (
                        self._anndata_obs[[field_name_1, field_name_2]]
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )
                    field_name_2_dict = (
                        co_oc.groupby(field_name_2)[field_name_1].apply(list).to_dict()
                    )
                    field_name_1_dict = (
                        co_oc.groupby(field_name_1)[field_name_2].apply(list).to_dict()
                    )
                    # co_oc["predicate"] = co_oc.apply(
                    #     lambda row: Predicate.CLUSTER_MATCHES.value
                    #     if row[field_name_2] in field_name_1_dict.get(row[field_name_1], [])
                    #     and len(field_name_1_dict.get(row[field_name_1], [])) == 1
                    #     else Predicate.SUBCLUSTER_OF.value
                    #     if row[field_name_2] in field_name_1_dict.get(row[field_name_1], [])
                    #     else Predicate.CLUSTER_OVERLAPS.value,
                    #     axis=1,
                    # )
                    co_oc["predicate"] = co_oc.apply(
                        lambda row: Predicate.CLUSTER_MATCHES.value
                        if field_name_2_dict.get(row[field_name_2], []) == [row[field_name_1]]
                        and field_name_1_dict.get(row[field_name_1], []) == [row[field_name_2]]
                        else Predicate.SUBCLUSTER_OF.value
                        if row[field_name_1] in field_name_2_dict.get(row[field_name_2], [])
                        and row[field_name_2] in field_name_1_dict.get(row[field_name_1], [])
                        and len(field_name_1_dict.get(row[field_name_1], [])) == 1
                        else Predicate.SUPERCLUSTER_OF.value
                        if row[field_name_1] in field_name_2_dict.get(row[field_name_2], [])
                        and row[field_name_2] in field_name_1_dict.get(row[field_name_1], [])
                        and len(field_name_2_dict.get(row[field_name_2], [])) == 1
                        else Predicate.CLUSTER_OVERLAPS.value,
                        axis=1,
                    )

                    temp_result.extend(co_oc.to_dict(orient="records"))

        result = [
            [item for sublist in [[k, v] for k, v in record.items()] for item in sublist]
            for record in temp_result
        ]

        return pd.DataFrame(
            [inner_list[:2] + inner_list[5:6] + inner_list[2:4] for inner_list in result],
            columns=["field_name1", "value1", "predicate", "field_name2", "value2"],
        )


class Predicate(Enum):
    CLUSTER_MATCHES = "cluster_matches"
    CLUSTER_OVERLAPS = "cluster_overlaps"
    SUBCLUSTER_OF = "subcluster_of"
    SUPERCLUSTER_OF = "supercluster_of"
