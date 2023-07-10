package site.ycsb.geodata;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class ReadFileWithGeoPointData {

    public String FILE_PATH;

    public ReadFileWithGeoPointData() {
        setFilePath("D:/Users/aliso/Documents/TCC/datas.csv");
    }

    public void setFilePath(String filePath) {
        this.FILE_PATH = filePath;
    }

    public ArrayList<GeoPoint> readFile(int recordcount) {
        ArrayList<GeoPoint> gps = new ArrayList<>();

        int counter = 0;
        try {
            File myFile = new File(FILE_PATH);

            try (Scanner myReader = new Scanner(myFile)) {
                myReader.nextLine(); // jump first line

                while (myReader.hasNextLine()) {

                    gps.add(stringToGeoPoint(myReader.nextLine()));
                    counter++;
                    if (counter >= recordcount)
                        break;
                }
            }

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
        }

        return gps;
    }

    private GeoPoint stringToGeoPoint(String content) {
        GeoPoint gp = new GeoPoint();

        String data[] = content.split(",");

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            String id = data[0];
            Date date = formatter.parse(data[3]);
            double latitude = Float.parseFloat(data[6]);
            double longitude = Float.parseFloat(data[7]);

            gp.setId(id+ new Date().getTime());
            gp.setTimeOfRecord(date);
            gp.setLatitude((latitude));
            gp.setLongitude((longitude));

        } catch (Exception error) {
            System.out.println("Error reading geopoints file: "+error.getMessage());
            System.exit(1);
        }

        return gp;
    }

}
