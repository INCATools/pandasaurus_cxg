import networkx as nx
from rdflib import OWL, RDF, RDFS, BNode, Graph, Literal, Namespace, URIRef

from pandasaurus_cxg.graph_generator.graph_predicates import (
    CLUSTER,
    CONSIST_OF,
    SUBCLUSTER_OF,
)

colour_mapping = {
    "http://www.w3.org/2002/07/owl#Class": "deepskyblue",
    "http://purl.obolibrary.org/obo/PCL_0010001": "cyan",
}


def add_edge(nx_graph: nx.Graph, subject, predicate, obj):
    edge_data = {
        "label": (
            CONSIST_OF["label"]
            if str(predicate) == CONSIST_OF["iri"]
            else SUBCLUSTER_OF["label"]
            if str(predicate) == SUBCLUSTER_OF["iri"]
            else CLUSTER["label"]
            if str(predicate) == CLUSTER["iri"]
            else str(predicate).split("#")[-1]
            if predicate and "#" in predicate
            else str(predicate).split("/")[-1]
        )
    }
    nx_graph.add_edge(
        str(subject),
        str(obj),
        **edge_data,
    )


def add_node(nx_graph: nx.Graph, subject, annotation):
    # nx_graph.add_node(str(subject), annotation=str(obj))
    nx_graph.add_node(str(subject), **annotation)


def add_outgoing_edges_to_subgraph(graph, predicate_uri=None):
    """
    Add all outgoing edges of a node in the graph to the subgraph.

    Parameters:
        graph (Graph): The RDF graph containing the triples.
        predicate_uri (URIRef or None): The predicate to filter triples (optional).

    Returns:
        rdflib.Graph: The subgraph containing the outgoing edges of the nodes.
    """
    subgraph = Graph()
    for s, p, o in graph.triples((None, predicate_uri, None)):
        subgraph.add((s, p, o))

    return subgraph


def find_and_rotate_center_layout(graph):
    """
    Find and rotate the center of the hierarchical tree layout.

    Parameters:
        graph (nx.Graph or nx.DiGraph): The graph to be visualized.

    Returns:
        dict: Rotated layout positions.
    """
    # Layout the graph as a hierarchical tree
    pos = nx.drawing.nx_agraph.graphviz_layout(graph, prog="dot")
    # Find the center of the layout
    x_sum, y_sum = 0, 0
    num_nodes = len(pos)
    for x, y in pos.values():
        x_sum += x
        y_sum += y
    x_center = x_sum / num_nodes
    y_center = y_sum / num_nodes
    # Reflect the positions with respect to the center to rotate by 180 degrees
    rotated_pos = {node: (2 * x_center - x, 2 * y_center - y) for node, (x, y) in pos.items()}
    return rotated_pos

  
def generate_subgraph(graph, predicate_uri, stack, bottom_up):
    subgraph = Graph()
    visited = set()
    while stack:
        node = stack.pop()
        if node not in visited:
            visited.add(node)
            for s, p, o in graph.triples((node, predicate_uri, None)):
                # Add all outgoing edges of the current node
                if isinstance(o, Literal) or p == RDF.type and not isinstance(o, BNode):
                    subgraph.add((s, p, o))
            if bottom_up:
                triples = graph.triples((node, predicate_uri, None))
            else:
                triples = graph.triples((None, predicate_uri, node))
            for s, p, o in triples:
                focused_node = o if bottom_up else s
                if not isinstance(focused_node, BNode):
                    stack.append(focused_node)
                    subgraph.add((s, p, o))
                else:
                    _s = next(graph.subjects(RDF.type, focused_node))
                    _p = next(graph.objects(focused_node, OWL.onProperty))
                    _o = next(graph.objects(focused_node, OWL.someValuesFrom))
                    subgraph.add((_s, _p, _o))
                    if bottom_up:
                        stack.append(_o)
                    else:
                        stack.append(_s)
    return subgraph

  
def select_node_with_property(graph: Graph, _property: str, value: str):
    ns = Namespace({k: v for k, v in graph.namespaces()}.get("ns"))
    if _property == "label":
        return [str(s) for s in graph.subjects(predicate=RDFS.label, object=Literal(value))]
    else:
        return [str(s) for s in graph.subjects(predicate=ns[_property], object=Literal(value))]
