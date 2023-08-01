import uuid
from enum import Enum
from typing import List, Optional

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from rdflib import OWL, RDF, RDFS, BNode, Graph, Literal, Namespace, URIRef

from pandasaurus_cxg.anndata_enricher import AnndataEnricher
from pandasaurus_cxg.utils.exceptions import (
    InvalidGraphFormat,
    MissingEnrichmentProcess,
)


class GraphGenerator:
    def __init__(
        self,
        co_annotation_report: pd.DataFrame,
        enricher: AnndataEnricher,
        keys: Optional[List[str]] = None,
    ):
        """
        Initializes GraphGenerator instance.

        Args:
            co_annotation_report (pd.DataFrame): The input DataFrame to generate rdf graph.
                Co-annotation report output.
            enricher (AnndataEnricher): Anndata enricher instance.
            keys (Optional[List[str]]): List of column names to select from the DataFrame.
                Defaults to None.

        """
        self.df = co_annotation_report[keys] if keys else co_annotation_report
        if enricher.enriched_df.empty:
            # TODO or we can just call simple_enrichment method
            enrichment_methods = [i for i in dir(AnndataEnricher) if "_enrichment" in i]
            enrichment_methods.sort()
            raise MissingEnrichmentProcess(enrichment_methods)
        else:
            self.enriched_df = enricher.enriched_df
        self.cell_type_dict = (
            enricher.get_anndata()
            .obs[["cell_type_ontology_term_id", "cell_type"]]
            .drop_duplicates()
            .set_index("cell_type_ontology_term_id")["cell_type"]
            .to_dict()
        )
        self.ns = Namespace("http://example.org/")
        self.graph = Graph()

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
        cell_set_class = self.ns["CellSet"]
        self.graph.add((cell_set_class, RDF.type, RDFS.Class))
        for _uuid, inner_dict in grouped_dict_uuid.items():
            resource = self.ns[_uuid]
            self.graph.add((resource, RDF.type, cell_set_class))
            for k, v in inner_dict.items():
                if k == "subcluster_of":
                    continue
                self.graph.add((resource, self.ns[k], Literal(v)))

        # add relationship between each resource based on their predicate in the co_annotation_report
        subcluster = self.ns["subcluster_of"]
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
        for curie, label in self.cell_type_dict.items():
            resource = cl_namespace[curie.split(":")[-1]]
            self.graph.add((resource, RDFS.label, Literal(label)))
            self.graph.add((resource, RDF.type, OWL.Class))
            for s, _, _ in self.graph.triples((None, self.ns["cell_type"], Literal(label))):
                # Add the triples to represent the restriction
                class_expression_bnode = BNode()
                self.graph.add((class_expression_bnode, RDF.type, OWL.Restriction))
                self.graph.add((class_expression_bnode, OWL.onProperty, self.ns["consist_of"]))
                self.graph.add((class_expression_bnode, OWL.someValuesFrom, resource))
                # Add the restriction
                self.graph.add((s, RDF.type, class_expression_bnode))
        # add subClassOf between terms in CL enrichment
        for _, row in self.enriched_df.iterrows():
            for s, _, _ in self.graph.triples((None, RDFS.label, Literal(row["s_label"]))):
                for o, _, _ in self.graph.triples((None, RDFS.label, Literal(row["o_label"]))):
                    self.graph.add((s, RDFS.subClassOf, o))

    def save_rdf_graph(
        self,
        graph: Optional[Graph],
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

        while stack:
            node = stack.pop()
            if node not in visited:
                visited.add(node)
            for s, p, o in graph.triples((node, self.ns[predicate] if predicate else None, None)):
                # Add all outgoing edges of the current node
                subgraph.add((s, p, o))
            for s, p, next_node in graph.triples(
                (node, self.ns[predicate] if predicate else None, None)
            ):
                if not isinstance(next_node, BNode):
                    stack.append(next_node)
                # else:
                #     subgraph.add(
                #         (
                #             node,
                #             next(graph.objects(next_node, OWL.onProperty)),
                #             next(graph.objects(next_node, OWL.someValuesFrom)),
                #         )
                #     )
                # TODO not sure if we need this else or not

        if not start_node:
            for s, p, o in graph.triples((None, self.ns[predicate] if predicate else None, None)):
                # Add all outgoing edges of the current node
                subgraph.add((s, p, o))

        nx_graph = nx.DiGraph()
        for subject, predicate, obj in subgraph:
            if isinstance(obj, URIRef) and predicate != RDF.type:
                edge_data = {"label": str(predicate).split("/")[-1]}
                nx_graph.add_edge(
                    str(subject).split("/")[-1],
                    str(obj).split("/")[-1],
                    **edge_data,
                )

        # Apply transitive reduction to remove redundancy
        transitive_reduction_graph = nx.transitive_reduction(nx_graph)
        transitive_reduction_graph.add_nodes_from(nx_graph.nodes(data=True))
        transitive_reduction_graph.add_edges_from(
            (u, v, nx_graph.edges[u, v]) for u, v in transitive_reduction_graph.edges
        )

        # Layout the graph as a hierarchical tree
        pos = nx.drawing.nx_agraph.graphviz_layout(transitive_reduction_graph, prog="dot")

        # Plot the graph as a hierarchical tree
        node_labels = nx.get_node_attributes(transitive_reduction_graph, "label")
        plt.figure(figsize=(10, 10))
        nx.draw(
            transitive_reduction_graph,
            pos,
            with_labels=True,
            labels=node_labels,
            node_size=1000,
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

    def transitive_reduction(self, predicate: str, file_path: str, _format: str = "xml"):
        graph = Graph().parse(file_path, format="ttl") if file_path else self.graph
        if predicate and not graph.query(f"ASK {{ ?s {self.ns[predicate].n3()} ?o }}"):
            raise ValueError(f"The {self.ns[predicate]} relation does not exist in the graph")

        subgraph = Graph()
        for s, p, o in graph.triples((None, self.ns[predicate] if predicate else None, None)):
            # Add all outgoing edges of the current node
            subgraph.add((s, p, o))

        nx_graph = nx.DiGraph()
        for subject, _predicate, obj in subgraph:
            if isinstance(obj, URIRef) and _predicate != RDF.type:
                edge_data = {
                    "label": "is_a" if _predicate == RDF.type else str(predicate).split("/")[-1]
                }
                nx_graph.add_edge(
                    str(subject).split("/")[-1],
                    str(obj).split("/")[-1],
                    **edge_data,
                )

        # Apply transitive reduction to remove redundancy
        transitive_reduction_graph = nx.transitive_reduction(nx_graph)
        transitive_reduction_graph.add_edges_from(
            (u, v, nx_graph.edges[u, v]) for u, v in transitive_reduction_graph.edges
        )

        # Remove redundant triples using nx graph
        edge_diff = list(set(nx_graph.edges) - set(transitive_reduction_graph.edges))

        for edge in edge_diff:
            if graph.query(
                f"ASK {{ {self.ns[edge[0]].n3()} {self.ns[predicate].n3()} {self.ns[edge[1]].n3()} }}"
            ):
                graph.remove((self.ns[edge[0]], self.ns[predicate], self.ns[edge[1]]))

        self.save_rdf_graph(graph, f"{file_path.split('.')[0]}_non_redundant", _format)


class RDFFormat(Enum):
    RDF_XML = "xml"
    TURTLE = "ttl"
    NTRIPLES = "nt"
