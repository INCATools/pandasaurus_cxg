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


def test_generate_rdf_graph_with_merge(graph_generator_instance_for_kidney):
    graph_generator = graph_generator_instance_for_kidney
    graph_generator.generate_rdf_graph(merge=True)
    assert len(graph_generator.graph) == 743
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
    assert len(graph_generator.graph) == 2173
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

    assert len(graph_generator.graph) == 743

    graph_generator.enrich_rdf_graph()

    assert len(graph_generator.graph) == 1242
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

    assert len(graph_generator.graph) == 2173

    graph_generator.enrich_rdf_graph()

    assert len(graph_generator.graph) == 2672


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
