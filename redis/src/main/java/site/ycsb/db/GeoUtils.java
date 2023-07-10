package site.ycsb.db;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import site.ycsb.geodata.GeoPoint;

public class GeoUtils {
    public static String dateToString(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(date);
    }

    public static Date stringtoDate(String dateString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();

        try {
            date = dateFormat.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return date;
    }

    public static double calculateMaxDistanceInKM(ArrayList<GeoPoint> gp) {
        double maxDistance = 0.0;

        for (int i = 0; i < gp.size() - 1; i++) {
            for (int j = i + 1; j < gp.size(); j++) {
                double lon1 = gp.get(i).getLongitude();
                double lat1 = gp.get(i).getLatitude();
                double lon2 = gp.get(j).getLongitude();
                double lat2 = gp.get(j).getLatitude();

                double earthRadius = 6371.0;

                double dLat = Math.toRadians(lat2 - lat1);
                double dLon = Math.toRadians(lon2 - lon1);

                double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                        Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLon / 2)
                                * Math.sin(dLon / 2);

                double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

                double distance = earthRadius * c;

                if (distance > maxDistance) {
                    maxDistance = distance;
                }
            }
        }

        return maxDistance;
    }

    public static boolean isCoordinateInsidePolygon(double x, double y, ArrayList<GeoPoint> gp) {
        int windingNumber = 0;
        int numPoints = gp.size();

        for (int i = 0; i < numPoints; i++) {
            double currentPointX = gp.get(i).getLongitude();
            double currentPointY = gp.get(i).getLatitude();
            double nextPointX = gp.get((i + 1) % numPoints).getLongitude();
            double nextPointY = gp.get((i + 1) % numPoints).getLatitude();

            if (currentPointY <= y) {
                if (nextPointY > y && isLeft(currentPointX, currentPointY, nextPointX, nextPointY, x, y) > 0) {
                    windingNumber++;
                }
            } else {
                if (nextPointY <= y && isLeft(currentPointX, currentPointY, nextPointX, nextPointY, x, y) < 0) {
                    windingNumber--;
                }
            }
        }

        return windingNumber != 0;
    }

    private static double isLeft(double x0, double y0, double x1, double y1, double x, double y) {
        return ((x1 - x0) * (y - y0)) - ((x - x0) * (y1 - y0));
    }
}
