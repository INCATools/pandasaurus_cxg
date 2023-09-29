import json
import os

import anndata as ad

# Load the .h5ad file
current_dir = os.path.dirname(os.path.abspath(__file__))
human_kidney = os.path.join(current_dir, "human_kidney.h5ad")

adata = ad.read_h5ad(human_kidney, backed="r")

# Modify the .uns["obs_meta"] dictionary
adata.uns["obs_meta"] = json.dumps(
    [
        {"field_name": "subclass.full", "field_type": "author_cell_type_label"},
        {"field_name": "subclass.l3", "field_type": "author_cell_type_label"},
        {"field_name": "subclass.l2", "field_type": "author_cell_type_label"},
        {"field_name": "subclass.l1", "field_type": "author_cell_type_label"},
        {"field_name": "class", "field_type": "author_cell_type_label"},
        {"field_name": "author_cell_type", "field_type": "author_cell_type_label"},
    ]
)

# Save the modified .h5ad file

file_path = os.path.join(current_dir, "x_modified_human_kidney.h5ad")
adata.write(file_path)
