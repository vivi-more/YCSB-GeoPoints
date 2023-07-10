package site.ycsb.geodata;
import java.util.Date;

public class GeoPoint{

    public GeoPoint(){}

    public GeoPoint(String id,
            double latitude,
            double longitude,
            Date timeOfReacord) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timeOfReacord = timeOfReacord;
    }

    private String id;
    private double latitude;
    private double longitude;
    private Date timeOfReacord;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getTimeOfRecord() {
        return timeOfReacord;
    }

    public void setTimeOfRecord(Date timeOfReacord) {
        this.timeOfReacord = timeOfReacord;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

}
