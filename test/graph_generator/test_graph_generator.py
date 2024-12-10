import os

import pandas as pd
import pytest
from rdflib import OWL, RDF, RDFS, BNode, Graph, Literal, Namespace, URIRef

from pandasaurus_cxg.enrichment_analysis import AnndataEnrichmentAnalyzer
from pandasaurus_cxg.graph_generator.graph_generator import GraphGenerator
from pandasaurus_cxg.graph_generator.graph_predicates import CONSIST_OF
from pandasaurus_cxg.utils.exceptions import (
    InvalidGraphFormat,
    MissingAnalysisProcess,
    MissingEnrichmentProcess,
)


@pytest.fixture()
def immune_file_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "../data/immune_example.h5ad")


@pytest.fixture()
def kidney_file_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, "../data/human_kidney.h5ad")


@pytest.fixture()
def enrichment_analyzer_instance_for_immune_data(immune_file_path):
    return AnndataEnrichmentAnalyzer(immune_file_path, author_cell_type_list=["author_cell_type"])


@pytest.fixture()
def enrichment_analyzer_instance_for_kidney_data(kidney_file_path):
    return AnndataEnrichmentAnalyzer(
        kidney_file_path,
        author_cell_type_list=[
            "subclass.full",
            "subclass.l3",
            "subclass.l2",
            "subclass.l1",
            "class",
            "author_cell_type",
        ],
    )


@pytest.fixture()
def graph_generator_instance_for_kidney(enrichment_analyzer_instance_for_kidney_data):
    ea = enrichment_analyzer_instance_for_kidney_data
    ea.enricher_manager.contextual_slim_enrichment()
    ea.co_annotation_report()
    return GraphGenerator(ea)


@pytest.fixture()
def expected_stable_ids():
    return {
        URIRef("http://example.org/6b8c6e7a-60e1-5272-bc96-6263fbcc5396"),
        URIRef("http://example.org/6637426a-b650-5d93-abee-99e8e52e763e"),
        URIRef("http://example.org/35a0d03d-7ce2-597e-8878-da67bb0b23b5"),
        URIRef("http://example.org/026ea297-be37-54a4-933e-29279c5385f1"),
        URIRef("http://example.org/d7d8ede3-841a-5b20-9cd3-d20a5aa4be63"),
        URIRef("http://example.org/70d96be3-736b-58c9-b8d8-ab57a1535698"),
        URIRef("http://example.org/040a09dc-9219-5812-8f0b-112935b0f21c"),
        URIRef("http://example.org/f28734b4-1c97-54a1-890f-0086dacbd7a3"),
        URIRef("http://example.org/61a31377-a156-538c-b24f-f0f4ec0c55bc"),
        URIRef("http://example.org/e239d685-8130-5260-a1cc-cae8875b916e"),
        URIRef("http://example.org/0e6fba5e-4d94-51ec-a413-7c4ff8b0192d"),
        URIRef("http://example.org/febe1fce-f128-5214-9acc-c98b1dc032d0"),
        URIRef("http://example.org/a584510c-57c1-5341-80c4-25e2357fb1cb"),
        URIRef("http://example.org/4aedaf72-be05-57a4-ad83-357742650904"),
        URIRef("http://example.org/910c088b-0f76-5440-b4af-4c5d1c575b6c"),
        URIRef("http://example.org/db34b5c9-921d-52bc-8f09-3393784fdd76"),
        URIRef("http://example.org/2e6aaf57-616b-5bc1-95e3-be985c9efddc"),
        URIRef("http://example.org/74b615a3-ced3-5cc3-9b34-6805b84c80cc"),
        URIRef("http://example.org/dd578556-05ed-59f6-b734-fb1bba5b3d67"),
        URIRef("http://example.org/7ae9cc3e-c9d4-5072-bb23-73703abbd61f"),
        URIRef("http://example.org/e215cbb5-ce0c-51b5-b146-06cac99d1d00"),
        URIRef("http://example.org/2bb80824-e368-5127-a5b0-512f4b495c14"),
        URIRef("http://example.org/38df699e-26e6-558f-a0c8-b6df9211adee"),
        URIRef("http://example.org/df2bb7b4-7154-52e4-8c02-13e8b8b43250"),
        URIRef("http://example.org/ffbacf1a-59f6-5aa2-bbf0-2327afa969a7"),
        URIRef("http://example.org/443b83af-ae41-5879-94fe-de7093ece241"),
        URIRef("http://example.org/85360cdc-6cff-52ac-b0bc-4b6f0b5fd5b3"),
        URIRef("http://example.org/409cb2da-d9d5-52f9-b1c1-af5a08ea6295"),
        URIRef("http://example.org/caf45aa6-2c7d-5ce4-9f46-2acb6c5a305f"),
        URIRef("http://example.org/daafa670-cc22-56dc-8fb3-3fba2390a350"),
        URIRef("http://example.org/b5687b1f-be71-5b31-914a-2070a129297b"),
        URIRef("http://example.org/1c4a6bb0-63d7-5102-813a-0e6378244a34"),
        URIRef("http://example.org/7ae5fcb4-5df8-5214-a150-2cfc6fa5c06c"),
        URIRef("http://example.org/646ca9dd-ffc6-5fb7-859b-3bbac7dbbdca"),
        URIRef("http://example.org/710ab24d-e2ed-56ee-855a-9ff086cb8a97"),
        URIRef("http://example.org/2c20973f-10c8-5ca4-8623-64e38d98458e"),
        URIRef("http://example.org/52e3fa21-f1ed-5ef7-b47e-5c4b875e18c2"),
        URIRef("http://example.org/ab520336-e633-5c4e-8086-c7e4e281a3fc"),
        URIRef("http://example.org/16b614ec-f0f1-5cc2-931d-2e8465b237ce"),
        URIRef("http://example.org/1ed21fba-45d3-5b43-93c0-0f9b27490b00"),
        URIRef("http://example.org/f1d3abc5-a927-5371-a3a2-61f35dc69ff2"),
        URIRef("http://example.org/a8682c5b-f4f1-5baf-9a7a-5987bbd678b9"),
        URIRef("http://example.org/f2ce8660-15be-5fbb-a53e-e43df111b5bf"),
        URIRef("http://example.org/511e7f73-91f2-5067-9dd4-f76b6ca093c7"),
        URIRef("http://example.org/943de3dd-3168-5e81-9e1a-298c1952ca31"),
        URIRef("http://example.org/51aef30d-c8f7-5d17-abcd-2c5bd3ba8570"),
        URIRef("http://example.org/0e116e68-2d16-56c3-9a38-43ae82834333"),
        URIRef("http://example.org/9652d787-b02c-521a-9374-a46295cd47dc"),
        URIRef("http://example.org/5bad5df9-d070-5403-a558-154b4e95168c"),
        URIRef("http://example.org/72a14bbd-7dbf-5f08-8f5b-cb5e778b239c"),
        URIRef("http://example.org/3f8e56f4-adde-564f-8274-b84df593873b"),
        URIRef("http://example.org/2f73506d-024a-572a-a202-08cf6222e5c6"),
        URIRef("http://example.org/9aaff9c9-a276-5260-bd7b-f570c4f4ac0a"),
        URIRef("http://example.org/c667ad1d-82f9-5b21-8bae-539a9e6d85d1"),
        URIRef("http://example.org/d549505a-3770-50f3-8af1-7f81035343a0"),
        URIRef("http://example.org/b87d3e84-20a3-50b1-87e2-f2ef66c505ba"),
        URIRef("http://example.org/ba2ee8d3-47a9-54c1-b14c-d1c92b905bb4"),
        URIRef("http://example.org/c4e5e19b-3440-5453-ae53-ac9d12348784"),
        URIRef("http://example.org/78914c73-b50b-5dd8-85f0-0af2b6584458"),
        URIRef("http://example.org/2b302895-6be3-5953-be23-2e51015ae18e"),
        URIRef("http://example.org/c99bfa83-ca47-58e6-9007-e984b17fc73d"),
        URIRef("http://example.org/177d7178-f313-5cc6-a31c-7207893a4d9a"),
        URIRef("http://example.org/c82e2caf-10c2-5ca2-822d-1c7e8fe60ac6"),
        URIRef("http://example.org/57fccc58-dc4c-5c18-b846-35e4a464f31a"),
        URIRef("http://example.org/3cf3e135-a988-52ac-aaae-bbf68ec3e059"),
        URIRef("http://example.org/f6cbfed7-3643-5907-a772-142902cfbcc5"),
        URIRef("http://example.org/2f7b8d0e-0fe0-5262-8218-2cee932617c4"),
        URIRef("http://example.org/2a1a7dd5-2344-5553-8c35-3c4ea7541e60"),
        URIRef("http://example.org/f598ef7d-b1c0-577d-9b73-7e9ae386d360"),
        URIRef("http://example.org/f9fdf3a4-82d7-5252-a209-755e69f12d01"),
        URIRef("http://example.org/07f0f2d0-3cf4-5a3a-8d05-dd9207a02a4d"),
        URIRef("http://example.org/a0d7cd01-04b2-539c-b729-21bc12cb18db"),
        URIRef("http://example.org/f472ede6-934f-5d16-a5ab-50fec5bea633"),
        URIRef("http://example.org/f9a44ae3-3b9f-5ed2-aee4-efeeccc1621c"),
        URIRef("http://example.org/fb25afeb-5c06-50c7-9334-577d898b800a"),
        URIRef("http://example.org/5979f500-3479-5ffb-9f04-4ae819a5972e"),
        URIRef("http://example.org/2a41fbaf-96f2-554f-b6ce-730279320ca6"),
        URIRef("http://example.org/f7e68907-b939-522e-b180-93695ae29d7e"),
        URIRef("http://example.org/4956d317-f11d-563e-91dc-2f582bd25ec4"),
        URIRef("http://example.org/7c45b81c-590a-54ed-90e2-3ea74baee514"),
        URIRef("http://example.org/c8cfedf2-9bfe-5c5f-a58f-b7d641d62005"),
        URIRef("http://example.org/583af61e-9c09-5f34-b8aa-8dfd31845521"),
        URIRef("http://example.org/bf6ce9ee-02d0-5c27-b2ab-99ec3b27b835"),
        URIRef("http://example.org/a0a64307-92df-5f08-9a0f-65bafa0ac9cb"),
        URIRef("http://example.org/c96bd95d-3274-582d-b306-26643f43920f"),
        URIRef("http://example.org/e9c3bd54-68ff-5ff9-909e-24a347bc00c1"),
        URIRef("http://example.org/fc9e7a46-3dd2-5abc-acbb-2803c1b0cee2"),
        URIRef("http://example.org/0e2d71c1-2be3-5add-8e31-a6138b03ab10"),
        URIRef("http://example.org/caeac903-9a94-5f8e-a84e-8d67f3c7a549"),
        URIRef("http://example.org/0ad4689d-b35a-5c69-8cc4-4c88f602b74d"),
        URIRef("http://example.org/845ffc96-dce4-5716-9fcf-6e161b83c1bd"),
        URIRef("http://example.org/9d841256-fc35-58b8-92a1-b327e9be2be6"),
        URIRef("http://example.org/3b747ebd-1c94-5e03-9514-e3c10195e746"),
        URIRef("http://example.org/4d0626e4-f2c8-53ec-83dd-794fd44ab20a"),
        URIRef("http://example.org/43aa9f66-d61b-59b2-a059-bdb03aaa082c"),
    }


def test_graph_generator_init_missing_analysis_process(
    enrichment_analyzer_instance_for_immune_data,
):
    ea = enrichment_analyzer_instance_for_immune_data
    ea.enricher_manager.simple_enrichment()

    with pytest.raises(MissingAnalysisProcess) as exc_info:
        GraphGenerator(ea)

    exception = exc_info.value
    expected_message = (
        "Any of the following analysis methods from AnndataAnalyser must be used first; "
        "co_annotation_report, enriched_co_annotation_report"
    )

    assert isinstance(exception, MissingAnalysisProcess)
    assert exception.args[0] == expected_message


def test_graph_generator_init_with_valid_input(enrichment_analyzer_instance_for_immune_data):
    ea = enrichment_analyzer_instance_for_immune_data
    ea.enricher_manager.simple_enrichment()
    ea.co_annotation_report()

    graph_generator = GraphGenerator(ea)

    assert graph_generator.ea == ea
    assert graph_generator.df.equals(ea.analyzer_manager.report_df)
    assert graph_generator.ns == Namespace("http://example.org/")
    assert graph_generator.graph is not None
    assert graph_generator.label_priority is None


def test_generate_rdf_graph_with_merge(graph_generator_instance_for_kidney, expected_stable_ids):
    graph_generator = graph_generator_instance_for_kidney
    graph_generator.generate_rdf_graph(merge=True)
    assert (
        set(
            graph_generator.graph.subjects(
                predicate=RDF.type, object=URIRef("http://purl.obolibrary.org/obo/PCL_0010001")
            )
        )
        == expected_stable_ids
    )
    assert len(graph_generator.graph) == 747
    assert (
        len([[s, p, o] for s, p, o in graph_generator.graph.triples((None, RDF.type, None))]) == 146
    )
    assert (
        len([[s, p, o] for s, p, o in graph_generator.graph.triples((None, RDFS.label, None))])
        == 21
    )
    assert (
        len(
            [
                [s, p, o]
                for s, p, o in graph_generator.graph.triples(
                    (None, URIRef("http://purl.obolibrary.org/obo/RO_0015003"), None)
                )
            ]
        )
        == 90
    )
    assert (
        len(
            [
                [s, p, o]
                for s, p, o in graph_generator.graph.triples(
                    (None, URIRef("http://example.org/cell_type"), None)
                )
            ]
        )
        == 16
    )

    graph_generator.graph = Graph().add(
        (URIRef("http://example.org/subject1"), RDF.type, OWL.Class)
    )
    assert len(graph_generator.graph) == 1
    graph_generator.generate_rdf_graph()
    assert len(graph_generator.graph) == 1


def test_generate_rdf_graph_without_merge(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney
    graph_generator.generate_rdf_graph()
    assert len(graph_generator.graph) == 2177
    assert (
        len([[s, p, o] for s, p, o in graph_generator.graph.triples((None, RDF.type, None))]) == 312
    )


def test_enrich_graph_missing_enrichment_process(enrichment_analyzer_instance_for_kidney_data):
    ea = enrichment_analyzer_instance_for_kidney_data
    ea.co_annotation_report()
    gg = GraphGenerator(ea)

    with pytest.raises(MissingEnrichmentProcess) as exc_info:
        gg.enrich_rdf_graph()

    exception = exc_info.value
    expected_message = (
        "Any of the following enrichment methods from AnndataEnricher must be used first; "
        "contextual_slim_enrichment, full_slim_enrichment, minimal_slim_enrichment, "
        "simple_enrichment"
    )

    assert isinstance(exception, MissingEnrichmentProcess)
    assert exception.args[0] == expected_message


def test_enrich_rdf_graph_with_merge(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney
    graph_generator.generate_rdf_graph(merge=True)

    assert len(graph_generator.graph) == 747

    graph_generator.enrich_rdf_graph()

    assert len(graph_generator.graph) == 1246
    assert (
        URIRef(CONSIST_OF.get("iri")),
        RDFS.label,
        Literal(CONSIST_OF.get("label")),
    ) in graph_generator.graph
    assert (None, RDF.type, OWL.Restriction) in graph_generator.graph
    assert (None, OWL.onProperty, URIRef(CONSIST_OF.get("iri"))) in graph_generator.graph
    assert (None, OWL.someValuesFrom, None) in graph_generator.graph
    assert (
        len(
            [
                s
                for s in graph_generator.graph.subjects()
                if str(s).startswith("http://purl.obolibrary.org/obo/CL_")
            ]
        )
        == 531
    )


def test_enrich_rdf_graph_without_merge(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney
    graph_generator.generate_rdf_graph()

    assert len(graph_generator.graph) == 2177

    graph_generator.enrich_rdf_graph()

    assert len(graph_generator.graph) == 2676


def test_save_rdf_graph(graph_generator_instance_for_kidney):
    graph = Graph()
    subject = URIRef("http://example.org/subject")
    predicate = URIRef("http://example.org/predicate")
    obj = Literal("Object")
    graph.add((subject, predicate, obj))

    graph_generator = graph_generator_instance_for_kidney
    graph_generator.save_rdf_graph(graph, "test_graph", "xml")

    file_path = "test_graph.owl"
    assert os.path.exists(file_path), "File was not saved."
    assert os.path.isfile(file_path), "Path is not a file."
    file_size = os.path.getsize(file_path)
    assert file_size > 0

    os.remove(file_path)

    with pytest.raises(InvalidGraphFormat) as exc_info:
        graph_generator.save_rdf_graph(graph, "test_graph", "invalid_format")

    exception = exc_info.value
    expected_message = "Graph format, invalid_format, provided for save_rdf_graph is invalid. Please use one of xml, ttl, nt"

    assert isinstance(exception, InvalidGraphFormat)
    assert exception.args[0] == expected_message


def test_transitive_reduction(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney
    graph_generator.generate_rdf_graph()
    # graph_generator.transitive_reduction()


def test_add_label_to_terms(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney

    # Define a sample RDF graph
    sample_graph = Graph()
    sample_graph.bind("ns", Namespace("http://example.org/"))

    subject1 = URIRef("http://example.org/subject1")
    subject2 = URIRef("http://example.org/subject2")
    predicate11 = URIRef("http://example.org/predicate11")
    predicate12 = URIRef("http://example.org/predicate12")
    predicate21 = URIRef("http://example.org/predicate21")
    predicate22 = URIRef("http://example.org/predicate22")
    literal11 = Literal("literal11")
    literal12 = Literal("literal12")
    literal21 = Literal("literal21")
    literal22 = Literal("literal22")

    sample_graph.add((subject1, RDF.type, OWL.Class))
    sample_graph.add((subject1, predicate11, literal11))
    sample_graph.add((subject1, predicate12, literal12))
    sample_graph.add((subject2, RDF.type, OWL.Class))
    sample_graph.add((subject2, predicate21, literal21))
    sample_graph.add((subject2, predicate22, literal22))

    assert len(sample_graph) == 6

    with pytest.raises(ValueError) as exc_info:
        graph_generator.add_label_to_terms(sample_graph)

    exception = exc_info.value
    expected_message = (
        "The priority order for adding labels is missing. "
        "Please use set_label_adding_priority method."
    )

    assert isinstance(exception, ValueError)
    assert exception.args[0] == expected_message

    graph_generator.set_label_adding_priority(
        {"predicate11": 4, "predicate12": 3, "predicate21": 2, "predicate22": 1}
    )
    graph_generator.add_label_to_terms(sample_graph)

    assert (subject1, RDFS.label, literal11) in sample_graph
    assert (subject2, RDFS.label, literal21) in sample_graph


def test_set_label_adding_priority_list(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney

    label_priority_list = ["label1", "label2", "label3"]
    graph_generator.set_label_adding_priority(label_priority_list)

    assert graph_generator.label_priority == {
        "label1": 4,
        "label2": 3,
        "label3": 2,
        "cell_type": 1,
    }
    label_priority_list = ["cell_type", "label1", "label2", "label3"]
    graph_generator.set_label_adding_priority(label_priority_list)

    assert graph_generator.label_priority == {
        "cell_type": 4,
        "label1": 3,
        "label2": 2,
        "label3": 1,
    }


def test_set_label_adding_priority_dict(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney

    label_priority_dict = {"label1": 5, "label2": 3, "label3": 7}
    graph_generator.set_label_adding_priority(label_priority_dict)

    assert graph_generator.label_priority == label_priority_dict


def test_set_label_adding_priority_invalid(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney

    with pytest.raises(ValueError) as exc_info:
        invalid_priority = {3, 5}
        graph_generator.set_label_adding_priority(invalid_priority)

    exception = exc_info.value
    expected_message = "Invalid priority format"

    assert isinstance(exception, ValueError)
    assert exception.args[0] == expected_message

    with pytest.raises(ValueError) as exc_info:
        invalid_priority = {5: "label1"}
        graph_generator.set_label_adding_priority(invalid_priority)

    exception = exc_info.value
    expected_message = "Invalid types in priority dictionary"

    assert isinstance(exception, ValueError)
    assert exception.args[0] == expected_message
