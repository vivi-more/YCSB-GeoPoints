package site.ycsb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.geodata.GeoPoint;

public class JdbcDBGeoClient extends DB {

    /** The class to use as the jdbc driver. */
    public static final String DRIVER_CLASS = "db.driver";

    /** The URL to connect to the database. */
    public static final String CONNECTION_URL = "db.url";

    /** The user name to use to connect to the database. */
    public static final String CONNECTION_USER = "db.user";

    /** The password to use for establishing the connection. */
    public static final String CONNECTION_PASSWD = "db.passwd";

    public static final String PRIMARY_KEY = "ID";
    public static final String LONGITUDE_COLUMN = "LONGITUDE";
    public static final String LATITUDE_COLUMN = "LATITUDE";
    public static final String TIME_OF_RECORD_COLUMN = "TIME_OF_RECORD";

    // Spatial Reference System Identifier
    public static final String SRID = "4326";

    enum Type {
        INSERT(1), SCAN(2), SCAN_KNN(3),
        SCAN_DISTANCE(4), SCAN_POLYGON(5);

        private final int internalType;

        private Type(int type) {
            internalType = type;
        }
    }

    GeoDefaultDBFlavor geoDBFlavor;

    private Connection conn;
    private boolean initialized = false;
    private Properties props;
    private static final String DEFAULT_PROP = "";
    ConcurrentMap<Type, PreparedStatement> cachedStatements;
    long numRowsInBatch = 0;

    public JdbcDBGeoClient() {
        geoDBFlavor = new GeoDefaultDBFlavor();
    }

    @Override
    public void init() throws DBException, NumberFormatException {
        if (initialized) {
            System.err.println("Client connection already initialized.");
            return;
        }
        props = getProperties();
        String url = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
        String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
        String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
        String driver = props.getProperty(DRIVER_CLASS);

        try {
            if (driver != null) {
                Class.forName(driver);
            }

            System.out.println("Adding shard node URL: " + url);
            conn = DriverManager.getConnection(url, user, passwd);
            conn.setAutoCommit(true);

            cachedStatements = new ConcurrentHashMap<Type, PreparedStatement>();
        } catch (ClassNotFoundException e) {
            System.err.println("Error in initializing the JDBS driver: " + e);
            throw new DBException(e);
        } catch (SQLException e) {
            System.err.println("Error in database operation: " + e);
            throw new DBException(e);
        } catch (NumberFormatException e) {
            System.err.println("Invalid value for fieldcount property. " + e);
            throw new DBException(e);
        }

        initialized = true;
    }

    @Override
    public void cleanup() throws DBException {
        try {
            conn.close();
        } catch (SQLException e) {
            System.err.println("Error in closing the connection. " + e);
            throw new DBException(e);
        }
    }

    @Override
    public Status reset(String table) {
        try {
            String dropTableQuery = "DROP TABLE IF EXISTS "+table;
            PreparedStatement stmt = conn.prepareStatement(dropTableQuery);

            stmt.execute();

            String createTableQuery = "CREATE TABLE "+table+" (" +
                    "ID VARCHAR PRIMARY KEY," +
                    "TIME_OF_RECORD DATE," +
                    "LATITUDE FLOAT," +
                    "LONGITUDE FLOAT" +
                    ")";

            stmt = conn.prepareStatement(createTableQuery);
            stmt.execute();
            
            return Status.OK;
        } catch (SQLException e) {
            System.err.println("Error in processing reset of table " + table + ": " + e);
            return Status.ERROR;
        }
    }

    @Override
    public Status scanKNN(String table, double lat, double lgn, int k, ArrayList<GeoPoint> result) {
        try {
            PreparedStatement stmt = cachedStatements.get(Type.SCAN_KNN);

            if (stmt == null) {
                String query = geoDBFlavor.createScanKNNStatement(table);
                PreparedStatement ps = conn.prepareStatement(query);
                cachedStatements.putIfAbsent(Type.SCAN_KNN, ps);
                stmt = ps;
            }

            stmt.setDouble(1, lgn);
            stmt.setDouble(2, lat);
            stmt.setInt(3, k);

            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next()) {
                GeoPoint gp = new GeoPoint();
                gp.setId(resultSet.getString(PRIMARY_KEY));
                gp.setLatitude(resultSet.getDouble(LATITUDE_COLUMN));
                gp.setLongitude(resultSet.getDouble(LONGITUDE_COLUMN));
                gp.setTimeOfRecord(resultSet.getTimestamp(TIME_OF_RECORD_COLUMN));

                result.add(gp);
            }

            resultSet.close();
            return Status.OK;
        } catch (SQLException e) {
            System.err.println("Error in processing scanKNN of table " + table + ": " + e);
            return Status.ERROR;
        }
    }

    @Override
    public Status scanByDistance(String table, double lat, double lgn, double maxDistance,
            ArrayList<GeoPoint> result) {
        try {
            PreparedStatement stmt = cachedStatements.get(Type.SCAN_DISTANCE);

            if (stmt == null) {
                String query = geoDBFlavor.createScanDistanceStatement(table);
                PreparedStatement ps = conn.prepareStatement(query);
                cachedStatements.putIfAbsent(Type.SCAN_DISTANCE, ps);
                stmt = ps;
            }

            double maxDistanceInDegrees = maxDistance / 111.32;

            stmt.setDouble(1, lgn);
            stmt.setDouble(2, lat);
            stmt.setDouble(3, (maxDistanceInDegrees));

            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next()) {
                GeoPoint gp = new GeoPoint();
                gp.setId(resultSet.getString(PRIMARY_KEY));
                gp.setLatitude(resultSet.getDouble(LATITUDE_COLUMN));
                gp.setLongitude(resultSet.getDouble(LONGITUDE_COLUMN));
                gp.setTimeOfRecord(resultSet.getTimestamp(TIME_OF_RECORD_COLUMN));

                result.add(gp);
            }

            resultSet.close();
            return Status.OK;
        } catch (SQLException e) {
            System.err.println("Error in processing scan distance of table " + table + ": " + e);
            return Status.ERROR;
        }
    }

    @Override
    public Status scanByPolygon(String table, ArrayList<GeoPoint> polygonVertices,
            ArrayList<GeoPoint> result) {
        try {
            PreparedStatement stmt = cachedStatements.get(Type.SCAN_POLYGON);

            if (stmt == null) {
                String query = geoDBFlavor.createScanPolygonStatement(table);
                PreparedStatement ps = conn.prepareStatement(query);
                cachedStatements.putIfAbsent(Type.SCAN_POLYGON, ps);
                stmt = ps;
            }

            String polygon = "POLYGON((";

            for (GeoPoint gp : polygonVertices) {
                String latitude = Double.toString(gp.getLatitude());
                String longitude = Double.toString(gp.getLongitude());

                polygon += longitude + " " + latitude + ",";
            }

            polygon = polygon.substring(0, polygon.length() - 1);
            polygon += "))";

            stmt.setString(1, polygon);
            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next()) {
                GeoPoint gp = new GeoPoint();
                gp.setId(resultSet.getString(PRIMARY_KEY));
                gp.setLatitude(resultSet.getDouble(LATITUDE_COLUMN));
                gp.setLongitude(resultSet.getDouble(LONGITUDE_COLUMN));
                gp.setTimeOfRecord(resultSet.getTimestamp(TIME_OF_RECORD_COLUMN));

                result.add(gp);
            }

            resultSet.close();
            return Status.OK;
        } catch (SQLException e) {
            System.err.println("Error in processing scann polygon of table " + table + ": " + e);
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table, String key, GeoPoint gp) {
        try {
            PreparedStatement stmt = cachedStatements.get(Type.INSERT);

            if (stmt == null) {
                String query = geoDBFlavor.createInsertStatement(table);
                PreparedStatement ps = conn.prepareStatement(query);
                cachedStatements.putIfAbsent(Type.INSERT, ps);
                stmt = ps;
            }

            stmt.setString(1, key);
            stmt.setDouble(2, gp.getLongitude());
            stmt.setDouble(3, gp.getLatitude());
            stmt.setDate(4, new java.sql.Date(gp.getTimeOfRecord().getTime()));

            // Normal update
            int result = stmt.executeUpdate();
            // If we are not autoCommit, we might have to commit now

            if (result == 1) {
                return Status.OK;
            }

            return Status.UNEXPECTED_STATE;
        } catch (SQLException e) {
            System.err.println("Error in processing insert to table: " + table + e);
            return Status.ERROR;
        }
    }

    @Override
    public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
        return Status.ERROR;
    }

    @Override
    public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {
        return Status.ERROR;
    }

    @Override
    public Status update(String tableName, String key, Map<String, ByteIterator> values) {
        return Status.ERROR;
    }

    @Override
    public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
        return Status.ERROR;
    }

    @Override
    public Status delete(String tableName, String key) {
        return Status.ERROR;
    }

}
