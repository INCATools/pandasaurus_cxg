from typing import List, Optional
import uuid

import pandas as pd


class GraphGenerator:
    def __init__(self, dataframe: pd.DataFrame, keys: Optional[List[str]] = None):
        """
        Initializes GraphGenerator instance.

        Args:
            dataframe (pd.DataFrame): The input DataFrame.
            keys (Optional[List[str]]): List of column names to select from the DataFrame.
                Defaults to None.

        """
        self.df = dataframe[keys] if keys else dataframe

    def generate_rdf_graph(self):
        # preprocess
        column_group = ["field_name1", "value1"]
        df = self.df.sort_values(by=column_group).reset_index(drop=True)
        grouped_df = df[df["predicate"] == "cluster_matches"].groupby(column_group)
        # grouped_dict_uuid = {
        #     str(uuid.uuid4()): {
        #         **{
        #             inner_list[0][0]: inner_list[0][1],
        #             inner_list[0][3]: inner_list[0][4],
        #         },
        #         outer_key1: outer_key2,
        #     }
        #     for (outer_key1, outer_key2), inner_dict in grouped_df
        #     for inner_list in [inner_dict.values.tolist()]
        # }
        grouped_dict_uuid = {}
        for (outer_key1, outer_key2), inner_dict in grouped_df:
            uuid_key = str(uuid.uuid4())
            for inner_list in inner_dict.values.tolist():

                inner_dict_uuid = {
                    inner_list[0]: inner_list[1],
                    inner_list[3]: inner_list[4],
                }

                if uuid_key in grouped_dict_uuid:
                    grouped_dict_uuid[uuid_key].append(inner_dict_uuid)
                else:
                    grouped_dict_uuid[uuid_key] = [inner_dict_uuid]


        # generate a resource for each free-text cell_type annotation and cell_type_ontology_term annotation

        # add relationship between each resource based on their predicate in the co_annotation_report

        pass

    def save_rdf_graph(self):
        pass

    def visualize_rdf_graph(self):
        pass
