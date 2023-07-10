package site.ycsb.geodata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;
import site.ycsb.generator.AcknowledgedCounterGenerator;
import site.ycsb.generator.CounterGenerator;
import site.ycsb.generator.DiscreteGenerator;
import site.ycsb.generator.ExponentialGenerator;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.workloads.CoreWorkload;

public class GeoPointWorkload extends Workload {

    public static final String TABLENAME_PROPERTY_DEFAULT = "GEOPOINTS";

    public static final String SCAN_KNN_PROPORTION_PROPERTY = "scanknnproportion";
    public static final String SCAN_BY_DISTANCE_PROPORTION_PROPERTY = "scanbydistanceproportion";
    public static final String SCAN_BY_POLYGON_PROPORTION_PROPERTY = "scanbypolygonproportion";

    public static final String SCAN_KNN_PROPORTION_PROPERTY_DEFAULT = "0";
    public static final String SCAN_BY_DISTANCE_PROPORTION_PROPERTY_DEFAULT = "0";
    public static final String SCAN_BY_POLYGON_PROPORTION_PROPERTY_DEFAULT = "0";

    public static final String MIN_SCAN_NEIGHBORS_PROPERTY = "minscanneighbors";
    public static final String MAX_SCAN_NEIGHBORS_PROPERTY = "maxscanneighbors";

    public static final String MIN_SCAN_NEIGHBORS_PROPERTY_DEFAULT = "1";
    public static final String MAX_SCAN_NEIGHBORS_PROPERTY_DEFAULT = "10";

    public static final String MIN_SCAN_DISTANCE_PROPERTY = "minscandistance";
    public static final String MAX_SCAN_DISTANCE_PROPERTY = "maxscandistance";

    public static final String MIN_SCAN_DISTANCE_PROPERTY_DEFAULT = "1";
    public static final String MAX_SCAN_DISTANCE_PROPERTY_DEFAULT = "100";

    public static final String MIN_SCAN_POLYGON_DISTANCE_PROPERTY = "minscanpoligondistance";
    public static final String MAX_SCAN_POLYGON_DISTANCE_PROPERTY = "maxscanpoligondistance";

    public static final String MIN_SCAN_POLYGON_DISTANCE_PROPERTY_DEFAULT = "1";
    public static final String MAX_SCAN_POLYGON_DISTANCE_PROPERTY_DEFAULT = "100";

    protected String table;

    protected NumberGenerator keysequence;
    protected NumberGenerator keychooser;
    protected NumberGenerator scanneighbors;
    protected NumberGenerator scandistance;
    protected NumberGenerator scanpolygondistance;

    protected AcknowledgedCounterGenerator transactioninsertkeysequence;
    protected DiscreteGenerator operationchooser;

    protected long recordcount;
    protected int insertionRetryLimit;
    protected int insertionRetryInterval;

    protected ArrayList<GeoPoint> geopoints;
    protected ReadFileWithGeoPointData readerGeoPointFile;

    public GeoPointWorkload() {
        this.geopoints = new ArrayList<>();
        this.readerGeoPointFile = new ReadFileWithGeoPointData();
    }

    public void init(Properties p) throws WorkloadException {

        table = p.getProperty(CoreWorkload.TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

        recordcount = Long.parseLong(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));

        if (recordcount == 0) {
            recordcount = Integer.MAX_VALUE;
        }

        this.geopoints = readerGeoPointFile.readFile((int) recordcount);

        long insertstart = Long.parseLong(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

        int minscanneighbors = Integer
                .parseInt(p.getProperty(MIN_SCAN_NEIGHBORS_PROPERTY, MIN_SCAN_NEIGHBORS_PROPERTY_DEFAULT));

        int maxscanneighbors = Integer
                .parseInt(p.getProperty(MAX_SCAN_NEIGHBORS_PROPERTY, MAX_SCAN_NEIGHBORS_PROPERTY_DEFAULT));

        int minscandistance = Integer.parseInt(
                p.getProperty(MIN_SCAN_DISTANCE_PROPERTY, MIN_SCAN_DISTANCE_PROPERTY_DEFAULT));

        int maxscandistance = Integer.parseInt(
                p.getProperty(MAX_SCAN_DISTANCE_PROPERTY, MAX_SCAN_DISTANCE_PROPERTY_DEFAULT));

        int minscanpolygondistance = Integer.parseInt(
                p.getProperty(MIN_SCAN_POLYGON_DISTANCE_PROPERTY, MIN_SCAN_POLYGON_DISTANCE_PROPERTY_DEFAULT));

        int maxscanpolygondistance = Integer.parseInt(
                p.getProperty(MAX_SCAN_POLYGON_DISTANCE_PROPERTY, MAX_SCAN_POLYGON_DISTANCE_PROPERTY_DEFAULT));

        long insertcount = Integer.parseInt(
                p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));

        if (recordcount < (insertstart + insertcount)) {
            System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
            System.err.println("recordcount must be bigger than insertstart + insertcount.");
            System.exit(-1);
        }

        keysequence = new CounterGenerator(insertstart);
        operationchooser = createOperationGenerator(p);

        transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);

        keychooser = new UniformLongGenerator(insertstart, insertstart + insertcount - 1);

        scanneighbors = new UniformLongGenerator(minscanneighbors, maxscanneighbors);
        scandistance = new UniformLongGenerator(minscandistance, maxscandistance);
        scanpolygondistance = new UniformLongGenerator(minscanpolygondistance, maxscanpolygondistance);

        insertionRetryLimit = Integer.parseInt(p.getProperty(CoreWorkload.INSERTION_RETRY_LIMIT,
                CoreWorkload.INSERTION_RETRY_LIMIT_DEFAULT));
        insertionRetryInterval = Integer.parseInt(p.getProperty(CoreWorkload.INSERTION_RETRY_INTERVAL,
                CoreWorkload.INSERTION_RETRY_INTERVAL_DEFAULT));

    }

    private void doTransactionScanPolygon(DB db) {
        long keynum = nextKeynum();

        GeoPoint gp = geopoints.get((int) keynum);
        double verticesDistance = scanpolygondistance.nextValue().intValue();

        ArrayList<GeoPoint> gps = generateSquare(gp, verticesDistance);
        db.scanByPolygon(table, gps, new ArrayList<GeoPoint>());

    }

    private void doTransactionScanDistance(DB db) {
        long keynum = nextKeynum();

        GeoPoint gp = geopoints.get((int) keynum);
        double distance = scandistance.nextValue().intValue();

        db.scanByDistance(table, gp.getLatitude(), gp.getLongitude(), distance, new ArrayList<>());
    }

    private void doTransactionScanKNN(DB db) {
        long keynum = nextKeynum();

        GeoPoint gp = geopoints.get((int) keynum);
        int neighbors = scanneighbors.nextValue().intValue();
        db.scanKNN(table, gp.getLatitude(), gp.getLongitude(), neighbors, new ArrayList<>());
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        String operation = operationchooser.nextString();
        if (operation == null) {
            return false;
        }

        switch (operation) {
            case "SCAN_KNN":
                doTransactionScanKNN(db);
                break;
            case "SCAN_BY_DISTANCE":
                doTransactionScanDistance(db);
                break;
            case "SCAN_BY_POLYGON":
                doTransactionScanPolygon(db);
                break;
            default:
                break;
        }
        return true;
    }

    private static boolean isFirstThread = true;

    @Override
    public boolean doInsert(DB db, Object threadstate) {
        int keynum = keysequence.nextValue().intValue();
        Status status;

        synchronized (GeoPointWorkload.class) {
            if (isFirstThread) {
                isFirstThread = false;
                status = db.reset(table);
                if (!(null != status && status.isOk())) {
                    System.err.println("Error reseting DB");
                }
            }
        }

        GeoPoint gp = geopoints.get((int) keynum);
        String dbkey = gp.getId();

        int numOfRetries = 0;
        do {
            status = db.insert(table, dbkey, gp);
            if (null != status && status.isOk()) {
                break;
            }
            // Retry if configured. Without retrying, the load process will fail
            // even if one single insertion fails. User can optionally configure
            // an insertion retry limit (default is 0) to enable retry.
            if (++numOfRetries <= insertionRetryLimit) {
                System.err.println("Retrying insertion, retry count: " + numOfRetries);
                try {
                    // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
                    int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    break;
                }

            } else {
                System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
                        "Insertion Retry Limit: " + insertionRetryLimit);
                break;

            }
        } while (true);

        return null != status && status.isOk();
    }

    protected static DiscreteGenerator createOperationGenerator(final Properties p) {
        if (p == null) {
            throw new IllegalArgumentException("Properties object cannot be null");
        }

        final double scanknnproportion = Double
                .parseDouble(p.getProperty(SCAN_KNN_PROPORTION_PROPERTY, SCAN_KNN_PROPORTION_PROPERTY_DEFAULT));

        final double scanbydistanceproportion = Double.parseDouble(
                p.getProperty(SCAN_BY_DISTANCE_PROPORTION_PROPERTY, SCAN_BY_DISTANCE_PROPORTION_PROPERTY_DEFAULT));

        final double scanbypolygonproportion = Double.parseDouble(
                p.getProperty(SCAN_BY_POLYGON_PROPORTION_PROPERTY, SCAN_BY_POLYGON_PROPORTION_PROPERTY_DEFAULT));

        final DiscreteGenerator operationchooser = new DiscreteGenerator();

        if (scanknnproportion > 0) {
            operationchooser.addValue(scanknnproportion, "SCAN_KNN");
        }

        if (scanbydistanceproportion > 0) {
            operationchooser.addValue(scanbydistanceproportion, "SCAN_BY_DISTANCE");
        }

        if (scanbypolygonproportion > 0) {
            operationchooser.addValue(scanbypolygonproportion, "SCAN_BY_POLYGON");
        }

        return operationchooser;
    }

    private long nextKeynum() {
        long keynum;
        if (keychooser instanceof ExponentialGenerator) {
            do {
                keynum = transactioninsertkeysequence.lastValue() - keychooser.nextValue().intValue();
            } while (keynum < 0);
        } else {
            do {
                keynum = keychooser.nextValue().intValue();
            } while (keynum > transactioninsertkeysequence.lastValue());
        }
        return keynum;
    }

    public static ArrayList<GeoPoint> generateSquare(GeoPoint firstPoint, double distance) {
        double newLat = firstPoint.getLatitude() + (distance / 111.32);
        double newLong = firstPoint.getLongitude()
                + (distance / (111.32 * Math.cos(Math.toRadians(firstPoint.getLatitude())))) * -1;

        GeoPoint secondPoint = new GeoPoint("", newLat, newLong, new Date());

        newLat = secondPoint.getLatitude() + (distance / 111.32);
        newLong = secondPoint.getLongitude()
                + (distance / (111.32 * Math.cos(Math.toRadians(secondPoint.getLatitude()))));

        GeoPoint thirdPoint = new GeoPoint("", newLat, newLong, new Date());

        newLat = firstPoint.getLatitude() + (distance / 111.32);
        newLong = firstPoint.getLongitude()
                + (distance / (111.32 * Math.cos(Math.toRadians(firstPoint.getLatitude()))));

        GeoPoint fourthPoint = new GeoPoint("", newLat, newLong, new Date());

        return new ArrayList<>(Arrays.asList(firstPoint, secondPoint, thirdPoint, fourthPoint, firstPoint));
    }

}
