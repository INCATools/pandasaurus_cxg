from typing import List


class InvalidSlimName(Exception):
    def __init__(self, invalid_slim_list: List[str], valid_slim_list: list[dict[str, str]]):
        self.message = (
            f"The following slim names are invalid: {', '.join(invalid_slim_list)}. "
            f"Please use slims from: "
            f"{', '.join([slim.get('name') for slim in valid_slim_list])}."
        )
        super().__init__(self.message)


class CellTypeNotFoundError(Exception):
    def __init__(self, cell_type: str, cell_type_list: List[str]):
        self.message = (
            f"{cell_type} not found in the annotations."
            f"Please use cell types from: "
            f"{', '.join(cell_type_list)}."
        )
        super().__init__(self.message)


class SubclassWarning(Exception):
    def __init__(self, relation: List[List[str]]):
        joined_relations = ", ".join(["-".join(rel) for rel in relation])
        self.message = (
            f"The following cell type terms are related with subClassOf relation. "
            f"{joined_relations}."
        )
        super().__init__(self.message)
