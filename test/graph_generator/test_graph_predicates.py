from pandasaurus_cxg.graph_generator.graph_predicates import (
    CLUSTER,
    CONSIST_OF,
    SUBCLUSTER_OF,
)


def test_constants():
    # Check the values of CONSIST_OF dictionary
    assert CONSIST_OF["iri"] == "http://purl.obolibrary.org/obo/RO_0002473"
    assert CONSIST_OF["label"] == "composed primarily of"

    # Check the values of SUBCLUSTER_OF dictionary
    assert SUBCLUSTER_OF["iri"] == "http://purl.obolibrary.org/obo/RO_0015003"
    assert SUBCLUSTER_OF["label"] == "subcluster of"

    # Check the values of CLUSTER dictionary
    assert CLUSTER["iri"] == "http://purl.obolibrary.org/obo/PCL_0010001"
    assert CLUSTER["label"] == "cell cluster"
