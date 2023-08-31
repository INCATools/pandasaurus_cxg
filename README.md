# pandasaurus_cxg

STATUS: early Beta

A library for retreiving and leveraging the semantic context of ontogy annotation in [CxG standard](https://github.com/chanzuckerberg/single-cell-curation/blob/main/schema/3.0.0/schema.md) [AnnData files](https://anndata.readthedocs.io/en/latest/).

Slide summarising intended functionality
![image](https://github.com/INCATools/pandasaurus_cxg/assets/112839/3082dcd2-dd2f-469d-9076-4eabcc83130d)

## Installation

Available on [PyPi](https://pypi.org/project/pandasaurus-cxg/0.1.1/)

$ pip3 install pandasaurus_cxg

## Usage

`AnndataEnricher` and `AnndataAnalyzer` can be used individually as well as with `AnndataEnrichmentAnalyzer`, A wrapper class for AnndataEnricher and AnndataAnalyzer.
You can find a couple of examples below;

```python
from pandasaurus_cxg.anndata_enricher import AnndataEnricher
ade = AnndataEnricher.from_file_path("test/data/modified_human_kidney.h5ad")
ade.simple_enrichment()
ade.minimal_slim_enrichment(["blood_and_immune_upper_slim"])
```

```python
from pandasaurus_cxg.anndata_analyzer import AnndataAnalyzer
ada = AnndataAnalyzer.from_file_path("./immune_example.h5ad", author_cell_type_list = ['subclass.full', 'subclass.l3', 'subclass.l2', 'subclass.l1', 'class', 'author_cell_type'])
ada.co_annotation_report()
```

```python
from pandasaurus_cxg.enrichment_analysis import AnndataEnrichmentAnalyzer
from pandasaurus_cxg.graph_generator.graph_generator import GraphGenerator
aea = AnndataEnrichmentAnalyzer("test/data/modified_human_kidney.h5ad")
aea.contextual_slim_enrichment()
aea.co_annotation_report()
gg = GraphGenerator(aea)
gg.generate_rdf_graph()
gg.set_label_adding_priority(["class", "cell_type", "subclass.l1", "subclass.l1", "subclass.full", "subclass.l2", "subclass.l3"])
gg.add_label_to_terms()
gg.enrich_rdf_graph()
gg.save_rdf_graph(file_name="kidney_new", _format="ttl")
```
More examples and detailed explanation can be found in jupyter notebook given in [Snippets](#Snippets)

## Snippets

https://github.com/INCATools/pandasaurus_cxg/blob/roadmap/walkthrough.ipynb

## Roadmap

https://github.com/INCATools/pandasaurus_cxg/blob/roadmap/ROADMAP.md

