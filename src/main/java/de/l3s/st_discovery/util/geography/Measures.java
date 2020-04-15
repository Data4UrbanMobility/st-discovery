package de.l3s.st_discovery.util.geography;


import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class Measures {

    public static double distance(Geometry lhs, Geometry rhs) {
        Point lhsCenter = lhs.getCentroid();
        Point rhsCenter = rhs.getCentroid();

        double projectionX = (lhsCenter.getX() + rhsCenter.getX()) / 2;
        double projectionY = (lhsCenter.getY() + rhsCenter.getY()) / 2;

        try {
            String autoString = "AUTO2:42001,"+projectionX+","+projectionY;
            CoordinateReferenceSystem auto = CRS.decode(autoString);
            MathTransform transform = CRS.findMathTransform(DefaultGeographicCRS.WGS84, auto);

            Geometry lhsTrans = JTS.transform(lhs, transform);
            Geometry rhsTrans = JTS.transform(rhs, transform);

            return lhsTrans.distance(rhsTrans);
        } catch (FactoryException | TransformException e) {
            e.printStackTrace();
            return -1;
        }


    }

}
