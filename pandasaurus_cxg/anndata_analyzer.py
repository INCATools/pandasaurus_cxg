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
        result = []
        for text in free_text_cell_type:
            if text in self._anndata_obs:
                co_oc = (
                    self._anndata_obs[[text, "cell_type"]].drop_duplicates().reset_index(drop=True)
                )
                temp_result.extend(co_oc.to_dict(orient="records"))
        result = [
            [item for sublist in [[k, v] for k, v in record.items()] for item in sublist]
            for record in temp_result
        ]

        return pd.DataFrame(result, columns=["field_name1", "value1", "field_name2", "value2"])
