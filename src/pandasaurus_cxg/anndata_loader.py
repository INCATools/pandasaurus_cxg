import warnings

import anndata


class AnndataLoader:
    @staticmethod
    def load_from_file(file_path):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=anndata.ImplicitModificationWarning)
            try:
                anndata_obj = anndata.read(file_path, backed="r")
                return anndata_obj
            except Exception as e:
                print(f"An error occurred while loading the file: {e}")
                return None