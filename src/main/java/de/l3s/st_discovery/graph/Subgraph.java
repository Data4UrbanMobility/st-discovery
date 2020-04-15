package de.l3s.st_discovery.graph;

import de.l3s.st_discovery.model.regiongrowing.LabeledDefaultEdge;

import java.util.*;

public class Subgraph implements Iterable<LabeledDefaultEdge> {
    private static int idCounter = 0;

    Set<LabeledDefaultEdge> edges;
    private int id;

    public Subgraph() {
        edges = new HashSet<>();
        id = idCounter++;
    }

    public Subgraph(Set<LabeledDefaultEdge> edges) {
        this.edges = edges;
    }

    public void addEdge(LabeledDefaultEdge e) {
        edges.add(e);
    }

    public boolean isSubsetOf(Subgraph other) {
        return other.getEdges().containsAll(edges);
    }

    public Subgraph intersection(Subgraph other)  {
        Set<LabeledDefaultEdge> result = new HashSet<>(edges);
        result.retainAll(other.getEdges());

        return new Subgraph(result);
    }

    public Subgraph union(Subgraph other) {
        Set<LabeledDefaultEdge> result = new HashSet<>(edges);
        result.addAll(other.getEdges());

        return new Subgraph(result);
    }

    public double distance(Subgraph other) {
        //double dist = Double.POSITIVE_INFINITY;
        edges.stream().parallel().forEach(e1 -> {
            other.getEdges().stream().parallel().forEach(e2 -> {
            //for (LabeledDefaultEdge e2: other.getEdges()) {
                //dist = Math.min(dist, e1.distance(e2));
                e1.distance(e2);
            });
        });
        return -1;
    }

    @Override
    public Iterator<LabeledDefaultEdge> iterator() {
        return edges.iterator();
    }

    public void addAll(Subgraph other) {
        edges.addAll(other.getEdges());
    }


    public int size() {
        return edges.size();
    }

    public Set<LabeledDefaultEdge> getEdges() {
        return edges;
    }

    public int getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subgraph that = (Subgraph) o;
        return id == that.id &&
                Objects.equals(edges, that.edges);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
