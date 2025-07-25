from igraph import Graph

def ensure_label_column(g: Graph) -> None:
    """
    Ensure that each node in the graph has a 'label' attribute
    (the pure address in lowercase), derived from 'name'.

    Modifies g.vs in-place.
    """
    if "label" not in g.vs.attributes():
        g.vs["label"] = [name.split("_", 1)[1].lower() for name in g.vs["name"]]