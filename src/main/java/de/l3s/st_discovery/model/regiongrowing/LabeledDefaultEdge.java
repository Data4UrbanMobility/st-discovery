/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.l3s.st_discovery.model.regiongrowing;

import de.l3s.st_discovery.util.geography.Measures;
import org.jgrapht.graph.DefaultEdge;
import org.locationtech.jts.geom.Geometry;

/**
 *
 * @author Udo
 */
   public class LabeledDefaultEdge extends DefaultEdge{
        
        public int label;
        public int node_sink, node_source;
        public int clusterId;
        private Geometry geometry;

        public LabeledDefaultEdge(){
            super();
        }

    public void setGeometry(Geometry geometry) {
        this.geometry = geometry;
    }

    public double distance(LabeledDefaultEdge other) {
        return Measures.distance(geometry, other.geometry);
    }

    public Geometry getGeometry() {
        return geometry;
    }

    @Override
    public int hashCode() {
        return label;
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
        final LabeledDefaultEdge other = (LabeledDefaultEdge) obj;
        return label == other.label;
    }
        
        

   }