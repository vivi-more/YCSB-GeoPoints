package site.ycsb.geodata;

import java.util.ArrayList;
import site.ycsb.Status;

public abstract class GeoDBAbstarct {

        public abstract Status reset(String table);

        public abstract Status insert(String table, String key, GeoPoint values);

        public abstract Status scanKNN(String table, double lat, double lgn, int k, ArrayList<GeoPoint> result);

        public abstract Status scanByDistance(String table, double lat, double lgn, double maxDistance,
                        ArrayList<GeoPoint> result);

        public abstract Status scanByPolygon(String table, ArrayList<GeoPoint> polygonVertices,
                        ArrayList<GeoPoint> result);
}
