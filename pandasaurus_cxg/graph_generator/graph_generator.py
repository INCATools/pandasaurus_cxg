import textwrap
import uuid
from enum import Enum
from typing import Dict, List, Optional, Union

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from rdflib import OWL, RDF, RDFS, BNode, Graph, Literal, Namespace, URIRef
from rdflib.plugins.sparql import prepareQuery

from pandasaurus_cxg.enrichment_analysis import (
    AnndataEnricher,
    AnndataEnrichmentAnalyzer,
)
from pandasaurus_cxg.graph_generator.graph_generator_utils import (
    add_edge,
    add_node,
    add_outgoing_edges_to_subgraph,
    find_and_rotate_center_layout,
)
from pandasaurus_cxg.graph_generator.graph_predicates import (
    CLUSTER,
    CONSIST_OF,
    SUBCLUSTER_OF,
)
from pandasaurus_cxg.utils.exceptions import (
    InvalidGraphFormat,
    MissingEnrichmentProcess,
)
from pandasaurus_cxg.utils.logging_config import configure_logger

# Set up logger
logger = configure_logger()


class GraphGenerator:
    def __init__(
        self,
        enrichment_analyzer: AnndataEnrichmentAnalyzer,
        keys: Optional[List[str]] = None,
    ):
        """
        Initializes GraphGenerator instance.

        Args:
            enrichment_analyzer: A wrapper object for AnndataEnricher and AnndataAnalyzer.
            keys (Optional[List[str]]): List of column names to select from the DataFrame to
                generate the report. Defaults to None.

        """
        self.ea = enrichment_analyzer
        self.df = (
            self.ea.analyzer_manager.report_df[keys] if keys else self.ea.analyzer_manager.report_df
        )
        if self.ea.enricher_manager.enricher.enriched_df.empty:
            # TODO or we can just call simple_enrichment method
            enrichment_methods = [i for i in dir(AnndataEnricher) if "_enrichment" in i]
            enrichment_methods.sort()
            raise MissingEnrichmentProcess(enrichment_methods)
        self.cell_type_dict = (
            pd.concat(
                [
                    self.ea.enricher_manager.enricher.enriched_df[["s", "s_label"]],
                    self.ea.enricher_manager.enricher.enriched_df[["o", "o_label"]].rename(
                        columns={"o": "s", "o_label": "s_label"}
                    ),
                ],
                axis=0,
                ignore_index=True,
            )
            .drop_duplicates()
            .set_index("s")["s_label"]
            .to_dict()
        )
        self.ns = Namespace("http://example.org/")
        self.graph = Graph()
        self.label_priority = None

    def generate_rdf_graph(self):
        """
        Generates rdf graph using co_annotation report

        Returns:

        """
        if len(self.graph) != 0:
            return
        # preprocess
        column_group = ["field_name1", "value1"]
        df = self.df.sort_values(by=column_group).reset_index(drop=True)
        grouped_df = df.groupby(column_group)
        grouped_dict_uuid = {}
        for (_, _), inner_dict in grouped_df:
            temp_dict = {}
            for inner_list in inner_dict.values.tolist():
                if inner_list[2] == "cluster_matches":
                    inner_dict_uuid = {
                        inner_list[0]: inner_list[1],
                        inner_list[3]: inner_list[4],
                    }
                elif inner_list[2] == "subcluster_of":
                    inner_dict_uuid = {
                        inner_list[0]: inner_list[1],
                        "subcluster_of": {
                            inner_list[3]: inner_list[4],
                        },
                    }
                else:
                    inner_dict_uuid = {
                        inner_list[0]: inner_list[1],
                    }

                for key, value in inner_dict_uuid.items():
                    if key not in temp_dict.keys():
                        temp_dict.update({key: value})
                    elif key == "subcluster_of":
                        temp_dict.get("subcluster_of").update(value)

            if temp_dict not in grouped_dict_uuid.values():
                grouped_dict_uuid[str(uuid.uuid4())] = temp_dict

        # generate a resource for each free-text cell_type annotation and cell_type_ontology_term annotation
        # cell_set_class = self.ns["CellSet"]
        cell_set_class = URIRef(CLUSTER.get("iri"))
        self.graph.add((cell_set_class, RDF.type, OWL.Class))
        self.graph.add((cell_set_class, RDFS.label, Literal(CLUSTER.get("label"))))
        for _uuid, inner_dict in grouped_dict_uuid.items():
            resource = self.ns[_uuid]
            self.graph.add((resource, RDF.type, cell_set_class))
            for k, v in inner_dict.items():
                if k == "subcluster_of":
                    continue
                self.graph.add((resource, self.ns[k], Literal(v)))

        # add relationship between each resource based on their predicate in the co_annotation_report
        # subcluster = self.ns["subcluster_of"]
        subcluster = URIRef(SUBCLUSTER_OF.get("iri"))
        self.graph.add((subcluster, RDFS.label, Literal(SUBCLUSTER_OF.get("label"))))
        self.graph.add((subcluster, RDF.type, OWL.ObjectProperty))
        for _uuid, inner_dict in grouped_dict_uuid.items():
            resource = self.ns[_uuid]
            for ik, iv in inner_dict.get("subcluster_of", {}).items():
                predicate = URIRef(self.ns + ik)
                for s, _, _ in self.graph.triples((None, predicate, Literal(iv))):
                    self.graph.add((resource, subcluster, s))

    def enrich_rdf_graph(self):
        """
        Enrich RDF graph with enriched DataFrame from AnndataEnricher

        Returns:

        """
        # add cell_type nodes and consists_of relations
        cl_namespace = Namespace("http://purl.obolibrary.org/obo/CL_")
        consist_of = URIRef(CONSIST_OF.get("iri"))
        self.graph.add((consist_of, RDFS.label, Literal(CONSIST_OF.get("label"))))
        for curie, label in self.cell_type_dict.items():
            resource = cl_namespace[curie.split(":")[-1]]
            self.graph.add((resource, RDFS.label, Literal(label)))
            self.graph.add((resource, RDF.type, OWL.Class))
            for s, _, _ in self.graph.triples((None, self.ns["cell_type"], Literal(label))):
                # Add the triples to represent the restriction
                class_expression_bnode = BNode()
                self.graph.add((class_expression_bnode, RDF.type, OWL.Restriction))
                # self.graph.add((class_expression_bnode, OWL.onProperty, self.ns["consist_of"]))
                self.graph.add((class_expression_bnode, OWL.onProperty, consist_of))
                self.graph.add((class_expression_bnode, OWL.someValuesFrom, resource))
                # Add the restriction
                self.graph.add((s, RDF.type, class_expression_bnode))

        # add enrichment graph
        self.graph += self.ea.enricher_manager.enricher.graph

    def save_rdf_graph(
        self,
        graph: Optional[Graph] = None,
        file_name: Optional[str] = "mygraph",
        _format: Optional[str] = "xml",
    ):
        """
        Serializes and saves the RDF graph to a file.

        Args:
            graph: An optional RDF graph that will be serialized.
                If provided, this graph will be used for serialization.
                If not provided, the graph inside the GraphGenerator instance will be used.
            file_name: The name of the output file without the extension.
                Defaults to "mygraph".
            _format: The format of the RDF serialization. Defaults to "xml".

        Raises:
            InvalidGraphFormat: If the provided _format is not valid.

        """
        graph = graph if graph else self.graph
        format_extension = {
            RDFFormat.RDF_XML.value: "owl",
            RDFFormat.TURTLE.value: "ttl",
            RDFFormat.NTRIPLES.value: "nt",
        }

        if _format in format_extension:
            file_extension = format_extension[_format]
            graph.serialize(f"{file_name}.{file_extension}", format=_format)
        else:
            valid_formats = [valid_format.value for valid_format in RDFFormat]
            raise InvalidGraphFormat(RDFFormat, valid_formats)

    def visualize_rdf_graph(self, start_node: List[str], predicate: str, file_path: str):
        """
        Visualizes an RDF graph using NetworkX and Matplotlib, focusing on specified nodes and predicates.

        Args:
            start_node (List[str]): A list of starting node URIs for visualization.
                If provided, the visualization will focus on these nodes and their relationships.
            predicate (str): The predicate URI to visualize relationships.
                If provided, the visualization will show relationships with this predicate.
            file_path (str): Path to an RDF file in TTL format to load the graph from.
                If provided, the graph will be loaded from this file. If empty, the method
                will use the instance's 'graph' attribute.

        Raises:
            ValueError: If the 'predicate' does not exist in the graph or if none of the 'start_node'
                URIs exist in the RDF graph.

        Note:
            - The method uses NetworkX to create a hierarchical tree visualization of the RDF graph.
            - The visualization is focused on specific nodes and their relationships using the 'start_node'
              and 'predicate' parameters.
            - If the 'start_node' and 'predicate' parameter are not provided all graph will be visualized.
              Since it is not optimized to visualize all graph, it is not suggested to use without
              setting these parameters.

        """
        # TODO visualize all graph, with parametric annotation properties to better visualize the nodes.
        # TODO apply redundancy striping to owl directly
        graph = Graph().parse(file_path, format="ttl") if file_path else self.graph
        if predicate and not graph.query(f"ASK {{ ?s {self.ns[predicate].n3()} ?o }}"):
            raise ValueError(f"The {self.ns[predicate]} relation does not exist in the graph")
        if start_node:
            for node in start_node:
                if not URIRef(node) in graph.subjects():
                    raise ValueError(
                        f"None of the nodes in the list {node} exist in the RDF graph."
                    )
        visited = set()
        subgraph = Graph()
        stack = [URIRef(node) for node in start_node] if start_node else None
        predicate_uri = URIRef(predicate) if predicate else None

        while stack:
            node = stack.pop()
            if node not in visited:
                visited.add(node)
            for s, p, o in graph.triples((node, predicate_uri, None)):
                # Add all outgoing edges of the current node
                subgraph.add((s, p, o))
            for s, p, next_node in graph.triples((node, predicate_uri, None)):
                if not isinstance(next_node, BNode):
                    stack.append(next_node)
                else:
                    _p = next(graph.objects(next_node, OWL.onProperty))
                    _o = next(graph.objects(next_node, OWL.someValuesFrom))
                    subgraph.add(
                        (
                            node,
                            _p,
                            _o,
                        )
                    )
                    stack.append(_o)
                # TODO not sure if we need this else or not

        if not start_node:
            subgraph = add_outgoing_edges_to_subgraph(graph, predicate_uri)

        nx_graph = nx.DiGraph()
        for s, p, o in subgraph:
            if isinstance(o, URIRef) and p != RDF.type:
                add_edge(nx_graph, s, p, o)
            elif p == RDFS.label:
                add_node(nx_graph, s, o)

        # Apply transitive reduction to remove redundancy
        transitive_reduction_graph = nx.transitive_reduction(nx_graph)
        transitive_reduction_graph.add_nodes_from(nx_graph.nodes(data=True))
        transitive_reduction_graph.add_edges_from(
            (u, v, nx_graph.edges[u, v]) for u, v in transitive_reduction_graph.edges
        )

        pos = find_and_rotate_center_layout(transitive_reduction_graph)
        plt.figure(figsize=(10, 10))

        # Plot the graph as a hierarchical tree
        node_labels = nx.get_node_attributes(transitive_reduction_graph, "label")
        node_labels = {
            node: "\n".join(textwrap.wrap(label, width=10)) for node, label in node_labels.items()
        }
        nx.draw(
            transitive_reduction_graph,
            pos,
            with_labels=True,
            labels=node_labels,
            node_size=2000,
            node_color="skyblue",
            font_size=8,
            font_weight="bold",
        )
        # Draw edge labels on the graph
        edge_labels = nx.get_edge_attributes(transitive_reduction_graph, "label")
        edge_labels_formatted = {edge: label for edge, label in edge_labels.items()}
        nx.draw_networkx_edge_labels(
            transitive_reduction_graph,
            pos,
            edge_labels=edge_labels_formatted,
            font_size=8,
            font_color="red",
        )
        plt.show()

    def transitive_reduction(self, predicate_list: List[str], file_path: str, _format: str = "xml"):
        # TODO We do not need this anymore since it is moved to pandasaurus
        graph = Graph().parse(file_path, format="ttl") if file_path else self.graph
        invalid_predicates = []
        for predicate in predicate_list:
            if predicate and not graph.query(f"ASK {{ ?s <{predicate}> ?o }}"):
                invalid_predicates.append(predicate)
                continue

            predicate_uri = URIRef(predicate) if predicate else None
            subgraph = add_outgoing_edges_to_subgraph(graph, predicate_uri)

            nx_graph = nx.DiGraph()
            for s, p, o in subgraph:
                if isinstance(o, URIRef) and p != RDF.type:
                    add_edge(nx_graph, s, predicate, o)

            # Apply transitive reduction to remove redundancy
            transitive_reduction_graph = nx.transitive_reduction(nx_graph)
            transitive_reduction_graph.add_edges_from(
                (u, v, nx_graph.edges[u, v]) for u, v in transitive_reduction_graph.edges
            )
            # Remove redundant triples using nx graph
            edge_diff = list(set(nx_graph.edges) - set(transitive_reduction_graph.edges))
            for edge in edge_diff:
                if graph.query(f"ASK {{ <{edge[0]}> <{predicate}> <{edge[1]}> }}"):
                    graph.remove((URIRef(edge[0]), URIRef(predicate), URIRef(edge[1])))
            logger.info(f"Transitive reduction has been applied on {predicate}.")

        self.save_rdf_graph(graph, f"{file_path.split('.')[0]}_non_redundant", _format)
        logger.info(f"{file_path.split('.')[0]}_non_redundant has been saved.")

        if invalid_predicates:
            error_msg = (
                f"The predicate '{invalid_predicates[0]}' does not exist in the graph"
                if len(invalid_predicates) == 1
                else f"The predicates {' ,'.join(invalid_predicates)} do not exist in the graph"
            )
            logger.error(error_msg)

    def add_label_to_terms(self, graph_: Graph = None):
        if not self.label_priority:
            raise ValueError(
                "The priority order for adding labels is missing. Please use set_label_adding_priority method."
            )
        graph = graph_ if graph_ else self.graph
        # TODO have a better way to handle priority assignment and have an auto default assignment
        # priority = {
        #     "subclass.l3": 1,
        #     "subclass.l2": 2,
        #     "subclass.full": 3,
        #     "subclass.l1": 4,
        #     "cell_type": 5,
        #     "class": 6,
        # }
        priority = self.label_priority
        unique_subjects_query = (
            "SELECT DISTINCT ?subject WHERE { ?subject ?predicate ?object FILTER (isIRI(?subject))}"
        )
        properties_query = prepareQuery(
            "SELECT ?predicate ?object WHERE { ?subject ?predicate ?object. filter (isLiteral(?object) && ?predicate != rdfs:label)}"
        )
        for result in graph.query(unique_subjects_query):
            resource = result.subject
            label_field = (None, 0)
            for properties_result in graph.query(
                properties_query, initBindings={"subject": result.subject}, initNs={"rdfs": RDFS}
            ):
                predicate = properties_result.predicate
                object_ = properties_result.object
                priority_value = priority.get(str(predicate).split("/")[-1], 0)
                if priority_value > label_field[1]:
                    label_field = [str(object_), priority_value]
            if label_field[0]:
                graph.add((resource, RDFS.label, Literal(label_field[0])))

    def set_label_adding_priority(self, label_priority: Union[List[str], Dict[str, int]]):
        """
        Set the priority order for adding labels.

        Args:
            label_priority (Optional[Union[List[str], Dict[str, int]]]): Either a list of strings,
                a dictionary with string keys and int values, representing the priority
                order for adding labels.

        """
        if isinstance(label_priority, list):
            self.label_priority = {
                label: len(label_priority) - i for i, label in enumerate(label_priority)
            }

        elif isinstance(label_priority, dict):
            if all(
                isinstance(key, str) and isinstance(value, int)
                for key, value in label_priority.items()
            ):
                self.label_priority = label_priority
            else:
                raise ValueError("Invalid types in priority dictionary")

        else:
            raise ValueError("Invalid priority format")


class RDFFormat(Enum):
    RDF_XML = "xml"
    TURTLE = "ttl"
    NTRIPLES = "nt"
