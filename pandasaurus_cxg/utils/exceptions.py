from typing import List, Tuple


class InvalidSlimName(Exception):
    """
    Exception raised for invalid Slim names.

    Args:
        invalid_slim_list (List[str]): List of invalid Slim names.
        valid_slim_list (list[dict[str, str]]): List of valid Slim names and descriptions.

    Attributes:
        message (str): A descriptive error message.
    """

    def __init__(self, invalid_slim_list: List[str], valid_slim_list: list[dict[str, str]]):
        self.message = (
            f"The following slim names are invalid: {', '.join(invalid_slim_list)}. "
            f"Please use slims from: "
            f"{', '.join([slim.get('name') for slim in valid_slim_list])}."
        )
        super().__init__(self.message)


class InvalidGraphFormat(Exception):
    """
    Exception raised for an invalid graph format.

    Args:
        _format (str): The invalid graph format.
        valid_formats (List[str]): List of valid graph formats.

    Attributes:
        message (str): A descriptive error message.
    """

    def __int__(self, _format: str, valid_formats: List[str]):
        self.message = (
            f"Graph format, {_format}, provided for save_rdf_graph is invalid. "
            f"Please use one of {', '.join(valid_formats)}"
        )
        super().__init__(self.message)


class CellTypeNotFoundError(Exception):
    """
    Exception raised when specified cell types are not found in the annotation.

    Args:
        missing_cell_types (List[str]): List of missing cell types.
        cell_type_list (List[str]): List of available cell types.

    Attributes:
        message (str): A descriptive error message.
    """

    def __init__(self, missing_cell_types: List[str], cell_type_list: List[str]):
        self.message = (
            f"Following cell types not found in the annotation: {', '.join(missing_cell_types)}. "
            f"Please use cell types from: {', '.join(cell_type_list)}."
        )
        super().__init__(self.message)


class MissingEnrichmentProcess(Exception):
    """
    Exception raised when attempting to use enriched_rdf_graph method without prior enrichment.

    Args:
        enrichment_methods (List[str]): List of available enrichment methods.

    Attributes:
        message (str): A descriptive error message.
    """

    def __init__(self, enrichment_methods: List[str]):
        self.message = (
            f"Any of the following enrichment methods from AnndataEnricher must be used before "
            f"using enriched_rdf_graph method: "
            f"{', '.join(enrichment_methods)}"
        )
        super().__init__(self.message)


class SubclassWarning(Exception):
    """
    Warning raised when cell type terms are related with subClassOf relation.

    Args:
        relation (List[List[str]]): List of related cell type terms.

    Attributes:
        message (str): A descriptive warning message.
    """

    def __init__(self, relation: List[Tuple[str]]):
        joined_relations = ", ".join(["-".join(rel) for rel in relation])
        self.message = (
            f"The following cell type terms are related with subClassOf relation. "
            f"{joined_relations}."
        )
        super().__init__(self.message)
