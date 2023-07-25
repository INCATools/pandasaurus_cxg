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
                # self.graph.add((s, self.ns["consist_of"], resource))
        # add subClassOf between terms in CL enrichment
        for _, row in self.enriched_df.iterrows():
            for s, _, _ in self.graph.triples((None, RDFS.label, Literal(row["s_label"]))):
                for o, _, _ in self.graph.triples((None, RDFS.label, Literal(row["o_label"]))):
                    self.graph.add((s, RDFS.subClassOf, o))

    def save_rdf_graph(self, file_name: str = "mygraph", _format: str = "xml"):
        """
        Serializes and saves the RDF graph to a file.

        Args:
            file_name (str, optional): The name of the output file without the extension.
                Defaults to "mygraph".
            _format (str, optional): The format of the RDF serialization. Defaults to "xml".

        Raises:
            InvalidGraphFormat: If the provided _format is not valid.

        """
        format_extension = {
            RDFFormat.RDF_XML.value: "owl",
            RDFFormat.TURTLE.value: "ttl",
            RDFFormat.NTRIPLES.value: "nt",
        }

        if _format in format_extension:
            file_extension = format_extension[_format]
            self.graph.serialize(f"{file_name}.{file_extension}", format=_format)
        else:
            valid_formats = [valid_format.value for valid_format in RDFFormat]
            raise InvalidGraphFormat(RDFFormat, valid_formats)

    def visualize_rdf_graph(self, start_node: List[str], file_path: str):
        graph = Graph().parse(file_path, format="ttl") if file_path else self.graph
        for node in start_node:
            if not URIRef(node) in graph.subjects() or URIRef(node) in graph.objects():
                raise ValueError(f"None of the nodes in the list {node} exist in the RDF graph.")

        visited = set()
        stack = [URIRef(node) for node in start_node]
        subgraph = Graph()

        while stack:
            node = stack.pop()
            if node not in visited:
                visited.add(node)
                for subject, predicate, obj in graph.triples((node, None, None)):
                    if predicate != RDF.type:
                        subgraph.add(
                            (subject, predicate, obj)
                        )  # Add all outgoing edges of the current node
                for s, p, next_node in graph.triples((node, None, None)):
                    # stack.append(next_node)
                    if not isinstance(next_node, BNode):
                        stack.append(next_node)
                    else:
                        subgraph.add(
                            (
                                node,
                                next(graph.objects(next_node, OWL.onProperty)),
                                next(graph.objects(next_node, OWL.someValuesFrom)),
                            )
                        )

        nx_graph = nx.DiGraph()
        for subject, predicate, obj in subgraph:
            if isinstance(obj, URIRef):
                edge_data = {
                    "label": "is_a" if predicate == RDF.type else str(predicate).split("/")[-1]
                }
                nx_graph.add_edge(str(subject).split("/")[-1], str(obj).split("/")[-1], **edge_data)

        # Apply transitive reduction to remove redundancy
        transitive_reduction_graph = nx.transitive_reduction(nx_graph)
        transitive_reduction_graph.add_nodes_from(nx_graph.nodes(data=True))
        transitive_reduction_graph.add_edges_from(
            (u, v, nx_graph.edges[u, v]) for u, v in transitive_reduction_graph.edges
        )

        # Layout the graph as a hierarchical tree
        pos = nx.drawing.nx_agraph.graphviz_layout(transitive_reduction_graph, prog="dot")

        # Plot the graph as a hierarchical tree
        plt.figure(figsize=(10, 8))
        nx.draw(
            transitive_reduction_graph,
            pos,
            with_labels=True,
            node_size=1500,
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


class RDFFormat(Enum):
    RDF_XML = "xml"
    TURTLE = "ttl"
    NTRIPLES = "nt"
