package de.l3s.st_discovery.output;

public class DependencyRecord {

    public int subgraph1;
    public int subgraph2;
    public double distance;
    public double mutualInformation;
    public double score;

    public DependencyRecord(int subgraph1, int subgraph2, double distance, double mutualInformation, double score) {
        this.subgraph1 = subgraph1;
        this.subgraph2 = subgraph2;
        this.distance = distance;
        this.mutualInformation = mutualInformation;
        this.score = score;
    }
}
