import networkx as nx
import pytest
from rdflib import OWL, RDF, RDFS, BNode, Graph, Literal, Namespace, URIRef

from pandasaurus_cxg.graph_generator.graph_generator_utils import (
    add_edge,
    add_node,
    add_outgoing_edges_to_subgraph,
    find_and_rotate_center_layout,
    generate_subgraph,
    remove_special_characters,
    select_node_with_property,
)
from pandasaurus_cxg.graph_generator.graph_predicates import (
    CLUSTER,
    CONSIST_OF,
    SUBCLUSTER_OF,
)

# Define a sample RDF graph
sample_graph = Graph()
sample_graph.bind("ns", Namespace("http://example.org/"))

subject1 = URIRef("http://example.org/subject1")
subject2 = URIRef("http://example.org/subject2")
subject3 = URIRef("http://example.org/subject3")
subject4 = URIRef("http://example.org/subject4")
predicate1 = URIRef("http://example.org/predicate1")
predicate2 = URIRef("http://example.org/predicate2")
predicate3 = URIRef("http://example.org/predicate3")
object1 = URIRef("http://example.org/object1")
object2 = URIRef("http://example.org/object2")
literal1 = Literal("value3")
bnode = BNode()
cl_term = URIRef("http://purl.obolibrary.org/obo/CL_0000000")

sample_graph.add((subject1, RDF.type, OWL.Class))
sample_graph.add((subject2, RDF.type, OWL.Class))
sample_graph.add((subject2, RDF.type, OWL.Class))
sample_graph.add((subject1, RDFS.label, Literal("Label1")))
sample_graph.add((subject2, RDFS.label, Literal("Label2")))
sample_graph.add((subject1, predicate1, object1))
sample_graph.add((subject2, predicate2, object2))
sample_graph.add((subject2, predicate3, literal1))
sample_graph.add((subject1, predicate1, subject3))
sample_graph.add((subject3, predicate1, subject4))
sample_graph.add((subject2, predicate1, subject1))
sample_graph.add((bnode, RDF.type, OWL.Restriction))
sample_graph.add((bnode, OWL.onProperty, URIRef(CONSIST_OF.get("iri"))))
sample_graph.add((bnode, OWL.someValuesFrom, cl_term))
sample_graph.add((subject2, RDF.type, bnode))


def test_add_edge():
    graph = nx.Graph()
    add_edge(graph, subject1, CONSIST_OF["iri"], object1)
    add_edge(graph, subject2, SUBCLUSTER_OF["iri"], object2)
    assert len(graph.edges) == 2
    assert graph.get_edge_data(str(subject1), str(object1))["label"] == CONSIST_OF["label"]
    assert graph.get_edge_data(str(subject2), str(object2))["label"] == SUBCLUSTER_OF["label"]


def test_add_node():
    graph = nx.Graph()
    annotation = {"key1": "value1", "key2": "value2"}
    add_node(graph, subject1, annotation)
    assert len(graph.nodes) == 1
    assert graph.nodes[str(subject1)]["key1"] == "value1"
    assert graph.nodes[str(subject1)]["key2"] == "value2"


def test_add_outgoing_edges_to_subgraph():
    subgraph = add_outgoing_edges_to_subgraph(sample_graph, predicate1)
    assert len(subgraph) == 4
    assert (subject1, predicate1, object1) in subgraph


def test_find_and_rotate_center_layout():
    graph = nx.DiGraph()
    graph.add_edge(1, 2)
    graph.add_edge(1, 3)
    layout = find_and_rotate_center_layout(graph)
    assert layout[1] == (63.0, -6.0)
    assert layout[2] == (99.0, 66.0)
    assert layout[3] == (27.0, 66.0)


def test_generate_subgraph_bottom_up():
    stack = [URIRef("http://example.org/subject2")]
    bottom_up = True

    subgraph = generate_subgraph(sample_graph, None, stack, bottom_up)
    expected_triples = [
        [
            URIRef("http://example.org/subject2"),
            URIRef("http://purl.obolibrary.org/obo/RO_0002473"),
            URIRef("http://purl.obolibrary.org/obo/CL_0000000"),
        ],
        [
            URIRef("http://example.org/subject2"),
            URIRef("http://example.org/predicate1"),
            URIRef("http://example.org/subject1"),
        ],
        [
            URIRef("http://example.org/subject1"),
            URIRef("http://www.w3.org/2000/01/rdf-schema#label"),
            Literal("Label1"),
        ],
        [
            URIRef("http://example.org/subject1"),
            URIRef("http://example.org/predicate1"),
            URIRef("http://example.org/subject3"),
        ],
        [
            URIRef("http://example.org/subject1"),
            URIRef("http://example.org/predicate1"),
            URIRef("http://example.org/object1"),
        ],
        [
            URIRef("http://example.org/subject1"),
            URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            URIRef("http://www.w3.org/2002/07/owl#Class"),
        ],
        [
            URIRef("http://example.org/subject3"),
            URIRef("http://example.org/predicate1"),
            URIRef("http://example.org/subject4"),
        ],
        [
            URIRef("http://example.org/subject2"),
            URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
            URIRef("http://www.w3.org/2002/07/owl#Class"),
        ],
        [
            URIRef("http://example.org/subject2"),
            URIRef("http://www.w3.org/2000/01/rdf-schema#label"),
            Literal("Label2"),
        ],
        [
            URIRef("http://example.org/subject2"),
            URIRef("http://example.org/predicate2"),
            URIRef("http://example.org/object2"),
        ],
        [
            URIRef("http://example.org/subject2"),
            URIRef("http://example.org/predicate3"),
            Literal("value3"),
        ],
    ]
    assert len(subgraph) == 11
    for triple in expected_triples:
        assert triple in subgraph


def test_generate_subgraph_top_down():
    predicate_uri = URIRef("http://example.org/predicate1")
    stack = [URIRef("http://example.org/subject1")]
    bottom_up = False

    subgraph = generate_subgraph(sample_graph, predicate_uri, stack, bottom_up)

    assert len(subgraph) == 1
    assert (
        URIRef("http://example.org/subject2"),
        predicate_uri,
        URIRef("http://example.org/subject1"),
    ) in subgraph


def test_select_node_with_property_label():
    _property = "label"
    value = "Label1"

    result = select_node_with_property(sample_graph, _property, value)

    assert len(result) == 1
    assert "http://example.org/subject1" in result


def test_select_node_with_property_predicate():
    _property = "predicate3"
    value = "value3"

    result = select_node_with_property(sample_graph, _property, value)

    assert len(result) == 1
    assert "http://example.org/subject2" in result


@pytest.mark.parametrize(
    "input_string, expected_output",
    [
        ("Hello World!", "Hello_World"),
        ("123abc$%^", "123abc"),
        ("!@#$%^&*()_", "_"),
        ("_This_is_a_test_", "_This_is_a_test_"),
        ("", ""),
    ],
)
def test_remove_special_characters(input_string, expected_output):
    assert remove_special_characters(input_string) == expected_output
