import tabulate

from pandasaurus_cxg.enrichment_analysis import AnndataEnrichmentAnalyzer
from pandasaurus_cxg.graph_generator.graph_generator import GraphGenerator


aea = AnndataEnrichmentAnalyzer("test/data/modified_human_kidney.h5ad")
# aea = AnndataEnrichmentAnalyzer("test/data/human_kidney.h5ad")

print(aea.enricher_manager.contextual_slim_enrichment())

df = aea.analyzer_manager.co_annotation_report()
df = df.sort_values(["field_name1", "value1"]).reset_index(drop=True)
print(tabulate.tabulate(df, df.columns.tolist(), tablefmt="fancy_grid"))

gg = GraphGenerator(aea)
gg.generate_rdf_graph()
gg.set_label_adding_priority(["class", "cell_type", "subclass.l1", "subclass.l1", "subclass.full", "subclass.l2", "subclass.l3"])
gg.add_label_to_terms()
gg.enrich_rdf_graph()
gg.save_rdf_graph(file_name="sub", _format="ttl")
gg.visualize_rdf_graph(
    [
        "http://example.org/59611ea2-7626-41e4-ad9f-e18e4383ddff",
    ],
    predicate=None,
    file_path="sub.ttl",
)
