from typing import List


class InvalidSlimName(Exception):
    def __init__(self, invalid_slim_list: List[str], valid_slim_list: list[dict[str, str]]):
        self.message = (
            f"The following slim names are invalid: {', '.join(invalid_slim_list)}. "
            f"Please use slims from: "
            f"{', '.join([slim.get('name') for slim in valid_slim_list])}."
        )
        super().__init__(self.message)
