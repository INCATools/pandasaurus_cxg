from typing import List

import pandas as pd
from pandasaurus.query import Query

from pandasaurus_cxg.anndata_loader import AnndataLoader


class AnndataEnricher:

    # TODO think a better approach for cell_type_field and context_field selection/passing
    def __init__(
        self,
        file_path: str, cell_type_field: str = "cell_type_ontology_term_id",
        context_field: str = "tissue_ontology_term_id"
    ):
        self.__anndata = AnndataLoader.load_from_file(file_path)
        self.__seed_list = self.__anndata.obs[cell_type_field].unique().tolist()
        self.__enricher = Query(self.__seed_list)
        self.__context_list = None \
            if context_field not in self.__anndata.obs.keys() else self.__anndata.obs[context_field].unique().tolist()

    def simple_enrichment(self) -> pd.DataFrame:
        return self.__enricher.simple_enrichment()

    def minimal_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        return self.__enricher.minimal_slim_enrichment(slim_list)

    def full_slim_enrichment(self, slim_list: List[str]) -> pd.DataFrame:
        return self.__enricher.full_slim_enrichment(slim_list)

    def contextual_slim_enrichment(self) -> pd.DataFrame:
        return self.__enricher.contextual_slim_enrichment(self.__context_list) if self.__context_list else None

    def set_enricher_property_list(self, property_list: List[str]):
        self.__enricher = Query(self.__seed_list, property_list)


