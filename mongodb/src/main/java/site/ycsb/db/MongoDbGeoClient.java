package site.ycsb.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.Position;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.geodata.GeoPoint;

public class MongoDbGeoClient extends DB {

    private static final String PRIMARY_KEY = "ID";
    public static final String TIME_OF_RECORD_COLUMN = "TIME_OF_RECORD";
    public static final String LOCATION_COLUMN = "LOCATION";

    /** Used to include a field in a response. */
    private static final Integer INCLUDE = Integer.valueOf(1);
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

    private static String databaseName;
    private static MongoDatabase database;

    /** A singleton Mongo instance. */
    private static MongoClient mongoClient;

    /** The default read preference for the test. */
    private static ReadPreference readPreference;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The bulk inserts pending for the thread. */
    private final List<Document> bulkInserts = new ArrayList<Document>();

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one DB
     * instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (INIT_COUNT.decrementAndGet() == 0) {
            try {
                mongoClient.close();
            } catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: " + e1.toString());
                e1.printStackTrace();
                return;
            } finally {
                database = null;
                mongoClient = null;
            }
        }
    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        INIT_COUNT.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongoClient != null) {
                return;
            }

            Properties props = getProperties();

            // Just use the standard connection format URL
            // http://docs.mongodb.org/manual/reference/connection-string/
            // to configure the client.
            String url = props.getProperty("mongodb.url", null);
            boolean defaultedUrl = false;
            if (url == null) {
                defaultedUrl = true;
                url = "mongodb://localhost:27017/ycsb?w=1";
            }

            url = OptionsSupport.updateUrl(url, props);

            if (!url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
                System.err.println("ERROR: Invalid URL: '" + url
                        + "'. Must be of the form "
                        + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options' "
                        + "or 'mongodb+srv://<host>/database?options'. "
                        + "http://docs.mongodb.org/manual/reference/connection-string/");
                System.exit(1);
            }

            try {
                MongoClientURI uri = new MongoClientURI(url);

                String uriDb = uri.getDatabase();
                if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
                        && !"admin".equals(uriDb)) {
                    databaseName = uriDb;
                } else {
                    // If no database is specified in URI, use "ycsb"
                    databaseName = "ycsb";

                }

                readPreference = uri.getOptions().getReadPreference();
                writeConcern = uri.getOptions().getWriteConcern();

                mongoClient = new MongoClient(uri);
                database = mongoClient.getDatabase(databaseName)
                        .withReadPreference(readPreference)
                        .withWriteConcern(writeConcern);

                System.out.println("mongo client connection created with " + url);
            } catch (Exception e1) {
                System.err.println("Could not initialize MongoDB connection pool for Loader: "
                        + e1.toString());
                e1.printStackTrace();
                return;
            }
        }
    }

    @Override
    public Status reset(String table) {
        try {
            MongoCollection<Document> collection = database.getCollection(table);
            collection.deleteMany(new Document());
            
            collection.createIndex(new Document(LOCATION_COLUMN, "2dsphere"));
            return Status.OK;
        } catch (Exception e) {
            System.err.println("Exception while trying bulk reset with " + bulkInserts.size());
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table, String key, GeoPoint gp) {
        try {
            MongoCollection<Document> collection = database.getCollection(table);

            Position position = new Position(gp.getLongitude(), gp.getLatitude());
            Point point = new Point(position);

            Document toInsert = new Document(PRIMARY_KEY, key)
                    .append(LOCATION_COLUMN, point)
                    .append(TIME_OF_RECORD_COLUMN, gp.getTimeOfRecord());

            collection.insertOne(toInsert);

            return Status.OK;
        } catch (Exception e) {
            System.err.println("Exception while trying bulk insert with " + bulkInserts.size());
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status scanKNN(String table, double lat, double lgn, int k, ArrayList<GeoPoint> results) {
        try {

            MongoCollection<Document> collection = database.getCollection(table);

            Document query = new Document(LOCATION_COLUMN, new Document("$near", new Document("$geometry",
                    new Document("type", "Point").append("coordinates", Arrays.asList(lgn, lat)))));

            FindIterable<Document> docs = collection.find(query).limit(k);

            for (Document result : docs) {

                String resultId = result.getString(PRIMARY_KEY);
                Document locationDoc = result.get(LOCATION_COLUMN, Document.class);
                List<Double> coordinates = locationDoc.getList("coordinates", Double.class);

                GeoPoint geoPoint = new GeoPoint();
                geoPoint.setId(resultId);
                geoPoint.setLongitude(coordinates.get(0));
                geoPoint.setLatitude(coordinates.get(1));
                geoPoint.setTimeOfRecord(result.getDate(TIME_OF_RECORD_COLUMN));

                results.add(geoPoint);
            }
            return Status.OK;
        } catch (Exception e) {
            System.err.println(e.toString());
            return Status.ERROR;
        }
    }

    @Override
    public Status scanByDistance(String table, double lat, double lgn, double maxDistance,
            ArrayList<GeoPoint> results) {
        try {
            MongoCollection<Document> collection = database.getCollection(table);
            Document point = new Document("type", "Point").append("coordinates", Arrays.asList(lgn, lat));

            double distanceInMeters = maxDistance * 1000;
            Bson filter = Filters.near(LOCATION_COLUMN, point, (distanceInMeters), null);

            FindIterable<Document> docs = collection.find(filter);

            for (Document result : docs) {

                String resultId = result.getString(PRIMARY_KEY);
                Document locationDoc = result.get(LOCATION_COLUMN, Document.class);
                List<Double> coordinates = locationDoc.getList("coordinates", Double.class);

                GeoPoint geoPoint = new GeoPoint();
                geoPoint.setId(resultId);
                geoPoint.setLongitude(coordinates.get(0));
                geoPoint.setLatitude(coordinates.get(1));
                geoPoint.setTimeOfRecord(result.getDate(TIME_OF_RECORD_COLUMN));

                results.add(geoPoint);
            }

            return Status.OK;
        } catch (Exception e) {
            System.err.println(e.toString());
            return Status.ERROR;
        }
    }

    @Override
    public Status scanByPolygon(String table, ArrayList<GeoPoint> polygonVertices,
            ArrayList<GeoPoint> results) {
        try {
            MongoCollection<Document> collection = database.getCollection(table);
            List<Position> ps = polygonVertices.stream().map(gp -> new Position(gp.getLongitude(), gp.getLatitude()))
                    .collect(Collectors.toList());

            Polygon polygon = new Polygon(ps);
            Bson filter = Filters.geoWithin(LOCATION_COLUMN, polygon);

            List<Document> docs = collection.find(filter).into(new ArrayList<>());

            for (Document result : docs) {

                String resultId = result.getString(PRIMARY_KEY);
                Document locationDoc = result.get(LOCATION_COLUMN, Document.class);
                List<Double> coordinates = locationDoc.getList("coordinates", Double.class);

                GeoPoint geoPoint = new GeoPoint();
                geoPoint.setId(resultId);
                geoPoint.setLongitude(coordinates.get(0));
                geoPoint.setLatitude(coordinates.get(1));
                geoPoint.setTimeOfRecord(result.getDate(TIME_OF_RECORD_COLUMN));

                results.add(geoPoint);
            }

            return Status.OK;
        } catch (Exception e) {
            System.err.println(e.toString());
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        return Status.ERROR;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        return Status.ERROR;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
            Vector<HashMap<String, ByteIterator>> result) {
        return Status.ERROR;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        return Status.ERROR;
    }

    @Override
    public Status delete(String table, String key) {
        return Status.ERROR;
    }

}
