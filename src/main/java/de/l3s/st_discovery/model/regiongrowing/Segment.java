package de.l3s.st_discovery.model.regiongrowing;



/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import org.locationtech.jts.geom.Geometry;

/**
 *
 * @author Udo
 */
public class Segment {
    
    public int id;
    public int sourceNodeId, targetNodeId;
    public int clusterId, clusterIdTracked;
    public boolean isOutlier;
    public double trafficLoad;
    public Geometry geom;

    public Segment(int id, int sourceNodeId, int targetNodeId,  int clusterId, int clusterIdTracked, Geometry geom) {
        this.id = id;
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.clusterId = clusterId;
        this.clusterIdTracked = clusterIdTracked;
        this.isOutlier = false;
        this.trafficLoad = 0;
        this.geom=geom;
    }
    
    public Segment(int id) {
        this(id,-1, -1, -1, -1, null);
    }


    @Override
    public String toString(){
        String s = "";
        s += id;
        s += "," + "(" + sourceNodeId + "->" + targetNodeId + ")";
        s += "," + trafficLoad;
        s += "," + isOutlier;
        s += "," + clusterId;
        s += "," + clusterIdTracked;
        return s;
    }
    


    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return this.id == id;
    }



    
    
    
}
