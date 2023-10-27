from typing import List, Tuple


class InvalidSlimName(Exception):
    def __init__(self, invalid_slim_list: List[str], valid_slim_list: list[dict[str, str]]):
        self.message = (
            f"The following slim names are invalid: {', '.join(invalid_slim_list)}. "
            f"Please use slims from: "
            f"{', '.join([slim.get('name') for slim in valid_slim_list])}."
        )
        super().__init__(self.message)


class InvalidGraphFormat(Exception):
    def __init__(self, _format: str, valid_formats: List[str]):
        self.message = (
            f"Graph format, {_format}, provided for save_rdf_graph is invalid. "
            f"Please use one of {', '.join(valid_formats)}"
        )
        super().__init__(self.message)


class CellTypeNotFoundError(Exception):
    def __init__(self, missing_cell_types: List[str], cell_type_list: List[str]):
        self.message = (
            f"Following cell types not found in the annotation: {', '.join(missing_cell_types)}. "
            f"Please use cell types from: {', '.join(cell_type_list)}."
        )
        super().__init__(self.message)


class MissingEnrichmentProcess(Exception):
    def __init__(self, enrichment_methods: List[str]):
        self.message = (
            f"Any of the following enrichment methods from AnndataEnricher must be used first; "
            f"{', '.join(enrichment_methods)}"
        )
        super().__init__(self.message)


class MissingAnalysisProcess(Exception):
    def __init__(self, analysis_methods: List[str]):
        self.message = (
            f"Any of the following analysis methods from AnndataAnalyser must be used first; "
            f"{', '.join(analysis_methods)}"
        )
        super().__init__(self.message)


class SubclassWarning(Exception):
    def __init__(self, relation: List[Tuple[str, str]]):
        joined_relations = ", ".join(["-".join(rel) for rel in relation])
        self.message = (
            f"The following cell type terms are related with subClassOf relation. "
            f"{joined_relations}."
        )
        super().__init__(self.message)
