import warnings
from typing import Optional

import anndata


class AnndataLoader:
    @staticmethod
    def load_from_file(file_path: str) -> Optional[anndata.AnnData]:
        """Load anndata object from a file.

        Args:
            file_path: The path to the file containing the anndata object.

        Returns:
            The loaded anndata object if successful, else None.

        Note:
            - The method uses warnings to temporarily ignore ImplicitModificationWarning raised by anndata.
            - If an error occurs while loading the file, an error message is printed, and None is returned.
        """
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=anndata.ImplicitModificationWarning)
            try:
                anndata_obj = anndata.read(file_path, backed="r")
                return anndata_obj
            except Exception as e:
                print(f"An error occurred while loading the file: {e}")
                return None
