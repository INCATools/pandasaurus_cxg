from enum import Enum

import pandas as pd

from pandasaurus_cxg.anndata_loader import AnndataLoader
from pandasaurus_cxg.schema.schema_loader import read_json_file


class AnndataAnalyzer:
    def __init__(self, file_path: str, schema_path: str):
        self._anndata_obs = AnndataLoader.load_from_file(file_path).obs
        self._schema = read_json_file(schema_path)

    def co_annotation_report(self):
        free_text_cell_type = [key for key, value in self._schema.items() if value]
        temp_result = []
        for text in free_text_cell_type:
            if text in self._anndata_obs.columns:
                co_oc = (
                    self._anndata_obs[[text, "cell_type"]].drop_duplicates().reset_index(drop=True)
                )
                predicate_dict = co_oc.groupby("cell_type")[text].apply(list).to_dict()
                co_oc["predicate"] = co_oc.apply(
                    lambda row: Predicate.CLUSTER_MATCHES.value
                    if row[text] in predicate_dict.get(row["cell_type"], [])
                    and len(predicate_dict.get(row["cell_type"], [])) == 1
                    else (
                        Predicate.SUBCLUSTER_OF.value
                        if row[text] in predicate_dict.get(row["cell_type"], [])
                        else Predicate.CLUSTER_OVERLAPS.value
                    ),  # All the other cases should be marked with 'cluster_overlaps', right?
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
