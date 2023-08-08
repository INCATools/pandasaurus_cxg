from typing import Dict

import networkx as nx
from rdflib import Graph, URIRef


def add_edge(nx_graph, subject, predicate, obj):
    edge_data = {
        "label": str(predicate).split("#")[-1]
        if "#" in predicate
        else str(predicate).split("/")[-1]
    }
    nx_graph.add_edge(
        str(subject),
        str(obj),
        **edge_data,
    )


def add_node(nx_graph, subject, obj):
    nx_graph.add_node(str(subject), label=str(obj))


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
