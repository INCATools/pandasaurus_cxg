Introduction
------------

STATUS: early Beta

A library for retrieving and leveraging the semantic context of ontology annotation in [CxG standard](https://github.com/chanzuckerberg/single-cell-curation/blob/main/schema/3.0.0/schema.md) [AnnData files](https://anndata.readthedocs.io/en/latest/).

Slide summarising intended functionality
![image](https://github.com/INCATools/pandasaurus_cxg/assets/112839/3082dcd2-dd2f-469d-9076-4eabcc83130d)

Installation
=============

Available on [PyPi](https://pypi.org/project/pandasaurus-cxg/0.1.1/)

$ pip3 install pandasaurus_cxg

Usage
======

The `AnndataEnricher` and `AnndataAnalyzer` classes can be used both individually and in conjunction with the `AnndataEnrichmentAnalyzer` wrapper class. The `AnndataEnrichmentAnalyzer` class serves as a convenient way to leverage the functionalities of both `AnndataEnricher` and `AnndataAnalyzer`.

Using AnndataEnricher and AnndataAnalyzer Individually
======================================================

You can use the `AnndataEnricher` and `AnndataAnalyzer` classes separately to perform specific tasks on your data. For instance, `AnndataEnricher` facilitates data enrichment, while `AnndataAnalyzer` provides various analysis tools for Anndata objects.

.. code:: python

    from pandasaurus_cxg.anndata_enricher import AnndataEnricher
    ade = AnndataEnricher.from_file_path("test/data/modified_human_kidney.h5ad")
    ade.simple_enrichment()
    ade.minimal_slim_enrichment(["blood_and_immune_upper_slim"])

.. code:: python

    from pandasaurus_cxg.anndata_analyzer import AnndataAnalyzer
    ada = AnndataAnalyzer.from_file_path("./immune_example.h5ad", author_cell_type_list = ['subclass.full', 'subclass.l3', 'subclass.l2', 'subclass.l1', 'class', 'author_cell_type'])
    ada.co_annotation_report()


Using AnndataEnrichmentAnalyzer Wrapper
==========================================

The AnndataEnrichmentAnalyzer class wraps the functionality of both AnndataEnricher and AnndataAnalyzer, offering a seamless way to perform enrichment and analysis in one go.

.. code:: python

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

More examples and detailed explanation can be found in jupyter notebook given in `Snippets <#snippets>`_.

Snippets
=========

https://github.com/INCATools/pandasaurus_cxg/blob/roadmap/walkthrough.ipynb

Roadmap
=========

https://github.com/INCATools/pandasaurus_cxg/blob/roadmap/ROADMAP.md

