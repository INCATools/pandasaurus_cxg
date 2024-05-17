import textwrap
import uuid
from enum import Enum
from typing import Dict, List, Optional, Union

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from pandasaurus.graph.graph_generator import GraphGenerator as graphgen
from rdflib import OWL, RDF, RDFS, BNode, Graph, Literal, Namespace, URIRef
from rdflib.plugins.sparql import prepareQuery

from pandasaurus_cxg.enrichment_analysis import (
    AnndataAnalyzer,
    AnndataEnricher,
    AnndataEnrichmentAnalyzer,
)
from pandasaurus_cxg.graph_generator.graph_generator_utils import (
    add_edge,
    add_node,
    add_outgoing_edges_to_subgraph,
    colour_mapping,
    find_and_rotate_center_layout,
    generate_subgraph,
    remove_special_characters,
    select_node_with_property,
)
from pandasaurus_cxg.graph_generator.graph_predicates import (
    CLUSTER,
    CONSIST_OF,
    DATASET,
    HAS_SOURCE,
    SUBCLUSTER_OF,
)
from pandasaurus_cxg.utils.exceptions import (
    InvalidGraphFormat,
    MissingAnalysisProcess,
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
                Please refrain from using this parameter until the next notification

        """
        # TODO need to think about how to handle the requirement of enrichment and co_annotation_analysis methods
        self.ea = enrichment_analyzer
        if self.ea.analyzer_manager.report_df.empty:
            analysis_methods = [i for i in dir(AnndataAnalyzer) if "_report" in i]
            analysis_methods.sort()
            raise MissingAnalysisProcess(analysis_methods)
        # TODO need to handle invalid keys. We also need to discuss about keeping the keys param. DO NOT USE
        self.df = (
            enrichment_analyzer.analyzer_manager.report_df[keys]
            if keys
            else enrichment_analyzer.analyzer_manager.report_df
        )
        self.ns = Namespace("http://example.org/")
        self.graph = Graph()
        self.label_priority = None
        # TODO This part needs a better approach in the future
        self.graph.bind("ns", self.ns)
        self.graph.bind("obo", Namespace("http://purl.obolibrary.org/obo/"))

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

        # generate dataset entity and has_source property
        dataset_class = URIRef(self.ns[str(uuid.uuid4())])
        self.graph.add((dataset_class, RDF.type, URIRef(DATASET.get("iri"))))
        self.graph.add((dataset_class, RDFS.label, Literal(DATASET.get("label"))))
        uns = self.ea.enricher_manager.anndata.uns
        for key, value in uns.items():
            if not isinstance(value, str):
                continue
            self.graph.add((dataset_class, URIRef(self.ns[key]), Literal(value)))
        has_source = URIRef(HAS_SOURCE["iri"])
        self.graph.add((has_source, RDFS.label, Literal(HAS_SOURCE["label"])))

        # generate a resource for each free-text cell_type annotation and cell_type_ontology_term annotation
        cell_set_class = URIRef(CLUSTER.get("iri"))
        self.graph.add((cell_set_class, RDF.type, OWL.Class))
        self.graph.add((cell_set_class, RDFS.label, Literal(CLUSTER.get("label"))))
        for _uuid, inner_dict in grouped_dict_uuid.items():
            resource = self.ns[_uuid]
            self.graph.add((resource, RDF.type, cell_set_class))
            self.graph.add((resource, has_source, dataset_class))
            for k, v in inner_dict.items():
                if k == "subcluster_of":
                    continue
                self.graph.add((resource, self.ns[remove_special_characters(k)], Literal(v)))

        # add relationship between each resource based on their predicate in the co_annotation_report
        subcluster = URIRef(SUBCLUSTER_OF.get("iri"))
        self.graph.add((subcluster, RDFS.label, Literal(SUBCLUSTER_OF.get("label"))))
        self.graph.add((subcluster, RDF.type, OWL.ObjectProperty))
        for _uuid, inner_dict in grouped_dict_uuid.items():
            resource = self.ns[_uuid]
            for ik, iv in inner_dict.get("subcluster_of", {}).items():
                predicate = self.ns[remove_special_characters(ik)]
                for s, _, _ in self.graph.triples((None, predicate, Literal(iv))):
                    self.graph.add((resource, subcluster, s))

        # transitive reduction step
        self.graph = graphgen.apply_transitive_reduction(self.graph, [subcluster.toPython()])

        # add cell_type nodes and consists_of relations
        cl_namespace = Namespace("http://purl.obolibrary.org/obo/CL_")
        consist_of = URIRef(CONSIST_OF.get("iri"))
        self.graph.add((consist_of, RDFS.label, Literal(CONSIST_OF.get("label"))))
        for curie, label in self.ea.enricher_manager.seed_dict.items():
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

    def enrich_rdf_graph(self):
        """
        Enrich RDF graph with enriched DataFrame from AnndataEnricher

        Returns:

        """
        if self.ea.enricher_manager.enricher.enriched_df.empty:
            # TODO or we can just call simple_enrichment method
            enrichment_methods = [i for i in dir(AnndataEnricher) if "_enrichment" in i]
            enrichment_methods.sort()
            raise MissingEnrichmentProcess(enrichment_methods)
        # add enrichment graph, subClassOf relations
        self.graph += self.ea.enricher_manager.enricher.graph

    def add_metadata_nodes(self, metadata_fields: List[str]):
        """
        Add metadata nodes to an RDF graph based on the specified metadata fields. Each node represents a metadata
        attribute, and edges connecting these metadata nodes to cell clusters indicate the percentage contribution
        of each metadata to the cluster.

        This function modifies the internal state of the RDF graph by adding new nodes and edges.

        Args:
            metadata_fields (List[str]): A list of metadata field names that exist in the schema and should be added
                                         to the RDF graph as nodes.

        Returns:

        """
        obs = self.ea.enricher_manager.anndata.obs
        # metadata field validation
        # TODO schema should be involved
        missing_fields = [field for field in metadata_fields if field not in obs.keys()]
        if missing_fields:
            raise KeyError(f"Missing metadata fields: {', '.join(missing_fields)}")

        author_cell_types = list(self.ea.analyzer_manager.all_cell_type_identifiers)
        # remove 'cell_type' from all_cell_type_identifiers
        author_cell_types.pop(-1)
        # add an annotation property for percentage
        percentage_annotation_property = self.ns["percentage"]
        self.graph.add((percentage_annotation_property, RDF.type, OWL.AnnotationProperty))
        for metadata in metadata_fields:
            for s, _, _ in self.graph.triples((None, RDF.type, URIRef(CLUSTER.get("iri")))):
                for a_cell_type in author_cell_types:
                    literal = self.graph.value(subject=s, predicate=self.ns[a_cell_type])
                    if literal is None:
                        continue
                    percentages = (
                        obs[obs[a_cell_type] == str(literal)][metadata].value_counts(normalize=True)
                        * 100
                    ).loc[lambda x: x != 0.0]
                    for label, percentage in percentages.items():
                        annotated_target = self.graph.value(
                            predicate=RDFS.label, object=Literal(label)
                        )
                        if annotated_target is None:
                            annotated_target = URIRef(self.ns[str(uuid.uuid4())])
                            self.graph.add((annotated_target, RDF.type, self.ns[metadata]))
                            self.graph.add((annotated_target, RDFS.label, Literal(label)))
                        bnode_axiom = BNode()
                        self.graph.add((bnode_axiom, RDF.type, OWL.Axiom))
                        self.graph.add((bnode_axiom, OWL.annotatedSource, s))
                        self.graph.add(
                            (bnode_axiom, OWL.annotatedProperty, self.ns["has_" + metadata])
                        )
                        self.graph.add((bnode_axiom, OWL.annotatedTarget, annotated_target))
                        self.graph.add(
                            (
                                bnode_axiom,
                                percentage_annotation_property,
                                Literal("{:.2f}".format(percentage)),
                            )
                        )

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
            raise InvalidGraphFormat(_format, valid_formats)

    def visualize_rdf_graph(
        self,
        predicate: Optional[str] = None,
        start_node: Optional[List[str]] = None,
        node_selector: Optional[Dict[str, str]] = None,
        file_path: Optional[str] = None,
        bottom_up: Optional[bool] = True,
    ):
        """
        Visualizes an RDF graph using NetworkX and Matplotlib, focusing on specified nodes and predicates.

        Args:
            predicate: The predicate URI to visualize relationships. Defaults to None.
                If provided, the visualization will show relationships with this predicate.
            start_node: A list of starting node URIs for visualization. Defaults to None.
                If provided, the visualization will focus on these nodes and their relationships.
            node_selector: A dictionary specifying how to select a node when node URIs are not used.
                Defaults to None. The dictionary should have the following format:

                - To select by label:
                    {'property': 'label', 'value': 'x label'}

                - To select by annotation:
                    {'property': 'x_annotation', 'value': 'xxxx'}

                - The 'property' key represents the property that will be queried.
                - The 'value' key represents the desired property value to match.
            file_path: Path to an RDF file in TTL format to load the graph from. Defaults to None.
                If provided, the graph will be loaded from this file. If empty, the method
                will use the instance's 'graph' attribute.
            bottom_up: Determines the graph visualization approach. The default approach is
                bottom-up (default=True). Set it to False for a top-down approach.

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
        required_keys = {"property", "value"}
        if node_selector:
            if not required_keys.issubset(node_selector.keys()):
                raise ValueError("node_selector must contain 'property' and 'value' keys")
            start_node = select_node_with_property(
                graph, node_selector.get("property"), node_selector.get("value")
            )
        if start_node:
            for node in start_node:
                if not URIRef(node) in graph.subjects():
                    raise ValueError(
                        f"None of the nodes in the list {node} exist in the RDF graph."
                    )

        stack = [URIRef(node) for node in start_node] if start_node else None
        predicate_uri = URIRef(predicate) if predicate else None

        subgraph = generate_subgraph(graph, predicate_uri, stack, bottom_up)

        # TODO Discussion: Is it necessary to visualize a subgraph containing only the specified predicate if a start_node is not provided?
        if not start_node:
            subgraph = add_outgoing_edges_to_subgraph(graph, predicate_uri)

        nx_graph = nx.DiGraph()
        for s, p, o in subgraph:
            if isinstance(o, URIRef) and p != RDF.type:
                add_edge(nx_graph, s, p, o)
            elif p == RDFS.label:
                add_node(nx_graph, s, {"label": str(o)})
            elif p == RDF.type:
                add_node(nx_graph, s, {"type": str(o)})

        # Identify and remove nodes without any edge
        # cell cluster type generate a node independent of the whole graph. this fix it
        if len(nx_graph.nodes()) != 1:
            nodes_to_remove = [
                node for node, degree in dict(nx_graph.degree()).items() if degree == 0
            ]
            nx_graph.remove_nodes_from(nodes_to_remove)

        # Apply transitive reduction to remove redundancy
        transitive_reduction_graph = nx.transitive_reduction(nx_graph)
        transitive_reduction_graph.add_nodes_from(nx_graph.nodes(data=True))
        transitive_reduction_graph.add_edges_from(
            (u, v, nx_graph.edges[u, v]) for u, v in transitive_reduction_graph.edges
        )

        node_colors = []
        # Get node colors based on node types
        for node in transitive_reduction_graph.nodes:
            node_colors.append(
                colour_mapping.get(transitive_reduction_graph.nodes[node]["type"], "red")
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
            node_color=node_colors,
            font_size=8,
            font_weight="bold",
        )
        # Draw edge labels on the graph
        edge_labels = nx.get_edge_attributes(transitive_reduction_graph, "label")
        edge_labels = {
            edge: "\n".join(textwrap.wrap(label, width=10)) for edge, label in edge_labels.items()
        }
        # edge_labels_formatted = {edge: label for edge, label in edge_labels.items()}
        nx.draw_networkx_edge_labels(
            transitive_reduction_graph,
            pos,
            edge_labels=edge_labels,
            font_size=8,
            font_color="red",
        )
        plt.show()

    def add_label_to_terms(self, graph_: Graph = None):
        if not self.label_priority:
            raise ValueError(
                "The priority order for adding labels is missing. Please use set_label_adding_priority method."
            )
        graph = graph_ if graph_ else self.graph
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
            label_priority (Union[List[str], Dict[str, int]]): Either a list of strings,
                a dictionary with string keys and int values, representing the priority
                order for adding labels.

        """
        if isinstance(label_priority, list):
            # TODO Do we need to append the 'cell_type'?
            label_priority.append("cell_type") if "cell_type" not in label_priority else None
            self.label_priority = {
                label: len(label_priority) - i for i, label in enumerate(label_priority)
            }

        elif isinstance(label_priority, dict):
            if all(
                isinstance(key, str) and isinstance(value, int)
                for key, value in label_priority.items()
            ):
                # TODO Do we need to append the 'cell_type'?
                self.label_priority = label_priority
            else:
                raise ValueError("Invalid types in priority dictionary")

        else:
            raise ValueError("Invalid priority format")


class RDFFormat(Enum):
    RDF_XML = "xml"
    TURTLE = "ttl"
    NTRIPLES = "nt"
