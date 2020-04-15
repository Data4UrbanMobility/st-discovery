/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.l3s.st_discovery.model.regiongrowing.distancemeasures;

import java.util.Arrays;
import java.util.Collection;


/**
 *
 * @author feuerhake
 */
public class PointNd implements Cloneable{
    
    public double[] coords;
    
    public PointNd(){
        coords = null;
    }
    
    public PointNd(int dim){
        coords = new double[dim];
    }
    
    public PointNd(double[] coords){
        this.coords = coords;
    }
    
    public final void setCoordinates(double[] coords) {
        for (int i = 0; i < coords.length; i++) {
            this.coords[i] = coords[i];
        }
    }
    
    @Override
    public PointNd clone(){
        return new PointNd(coords.clone());
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 73 * hash + Arrays.hashCode(this.coords);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PointNd other = (PointNd) obj;
        if (!Arrays.equals(this.coords, other.coords)) {
            return false;
        }
        return true;
    }
        
    public double distance(PointNd p){
        double d = 0.0;
        for (int i = 0; i < coords.length; i++) {
            d += (coords[i] - p.coords[i]) * (coords[i] - p.coords[i]);
        }
        return Math.sqrt(d);
    }
    
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < coords.length; i++) {
            sb.append(coords[i]);
            sb.append(",");
        }
        return sb.toString();
    }
    
    public static PointNd getCenterPoint(Collection<PointNd> points) {
        PointNd point = null;
        for (PointNd p : points) {
            if(point == null){
                point = new PointNd(p.coords.length);
            }
            for (int i = 0; i < p.coords.length; i++) {
                point.coords[i] += p.coords[i] / (1.0 * points.size());
            }
        }
        return point;
    }
    
}
