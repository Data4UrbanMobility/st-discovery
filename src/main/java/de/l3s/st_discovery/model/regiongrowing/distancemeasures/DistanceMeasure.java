/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.l3s.st_discovery.model.regiongrowing.distancemeasures;


import de.l3s.st_discovery.model.regiongrowing.Segment;

/**
 *
 * @author feuerhake
 */
public interface DistanceMeasure {
        
        public double calculate(Segment segment, Segment segment2);

        @Override
        public String toString();
}

