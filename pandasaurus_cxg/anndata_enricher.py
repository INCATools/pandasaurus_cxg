from typing import List

import pandas as pd
from pandasaurus.query import Query

from pandasaurus_cxg.anndata_loader import AnndataLoader


class AnndataEnricher:

    # TODO think a better approach for cell_type_field and context_field selection/passing
    def __init__(
            self,
            file_path: str,
            cell_type_field: str = "cell_type_ontology_term_id",
            context_field: str = "tissue_ontology_term_id",
    ):
        self._anndata = AnndataLoader.load_from_file(file_path)
        self.__seed_list = self._anndata.obs[cell_type_field].unique().tolist()
        self.__enricher = Query(self.__seed_list)
        self.__context_list = (
            None
            if context_field not in self._anndata.obs.keys()
            else self._anndata.obs[context_field].unique().tolist()
        )
        self.enriched_df = pd.DataFrame()

    def simple_enrichment(self) -> pd.DataFrame:
        self.enriched_df = self.__enricher.simple_enrichment()
        return self.enriched_df

    def minimal_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        self.enriched_df = self.__enricher.minimal_slim_enrichment(slim_list)
        return self.enriched_df

    def full_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        self.enriched_df = self.__enricher.full_slim_enrichment(slim_list)
        return self.enriched_df

    def contextual_slim_enrichment(self) -> pd.DataFrame:
        self.enriched_df = (
            self.__enricher.contextual_slim_enrichment(self.__context_list)
            if self.__context_list
            else None
        )
        return self.enriched_df

    def filter_anndata_by_enriched_cell_type(self):
        # TODO 's' amd 'o' part should be revised
        cell_type_list = set(self.enriched_df['s'].tolist() + self.enriched_df['o'].tolist())
        return self._anndata.obs[self._anndata.obs['cell_type_ontology_term_id'].isin(cell_type_list)]

    def set_enricher_property_list(self, property_list: List[str]):
        self.__enricher = Query(self.__seed_list, property_list)
