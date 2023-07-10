package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.geodata.GeoPoint;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.neo4j.driver.*;

/**
 * Neo4j client for YCSB framework.
 */
public class Neo4jGeoClient extends DB {
  /** Path used to create the database directory. */
  public static final String HOST_PROP = "neo4j.host";
  public static final String DEFAULT_PROP_HOST = "bolt://localhost:7687";

  public static final String DEFAULT_DB = "neo4j.db";
  public static final String DEFAULT_PROP_DB = "GEOPOINTS";

  public static final String DEFAULT_PASSWORD = "neo4j.password";
  public static final String DEFAULT_PROP_PASSWORD = "12345678";

  private static final Lock insertLock = new ReentrantLock();
  private static final Lock polygonLock = new ReentrantLock();
  private static final Lock distanceLock = new ReentrantLock();
  private static final Lock knnLock = new ReentrantLock();

  /**
   * Default path used to create the database directory, if no arguments are
   * given.
   */
  /** The name of the node identifier field. */
  private static final String PRIMARY_KEY = "id";
  public static final String LONGITUDE_COLUMN = "longitude";
  public static final String LATITUDE_COLUMN = "latitude";
  public static final String TIME_OF_RECORD_COLUMN = "time_of_record";

  private static Driver driver;
  private static final ThreadLocal<Session> sessionHolder = new ThreadLocal<>();

  /** Integer used to keep track of current threads. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /**
   * Initializes the graph database, only once per DB instance.
   *
   * @throws DBException
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    if (driver == null) {
      synchronized (Neo4jGeoClient.class) {
        if (driver == null) {
          Properties props = getProperties();

          String host = props.getProperty(HOST_PROP, DEFAULT_PROP_HOST);
          String db = props.getProperty(DEFAULT_DB, DEFAULT_PROP_DB);
          String password = props.getProperty(DEFAULT_PASSWORD, DEFAULT_PROP_PASSWORD);

          driver = GraphDatabase.driver(host, AuthTokens.basic(db, password));
        }
      }
    }
    sessionHolder.set(driver.session());
  }

  @Override
  public void cleanup() throws DBException {
    Session session = sessionHolder.get();

    if (session != null) {
      session.close();
      sessionHolder.remove();
    }

    // Check if all threads have completed their work
    if (INIT_COUNT.decrementAndGet() == 0) {
      driver.close();
    }
  }

  @Override
  public Status reset(String table) {
    Transaction tx = null;

    try {
      Session session = sessionHolder.get();
      tx = session.beginTransaction();
      tx.run("MATCH (n) DETACH DELETE n;");
      tx.run("CALL spatial.addPointLayer('geom');");

      tx.commit();

      return Status.OK;
    } catch (Exception e) {

      System.err.println("Failed to reset data: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, GeoPoint values) {
    Transaction tx = null;
    try {
      Session session = sessionHolder.get();
      tx = session.beginTransaction();

      insertLock.lock();
      StringBuilder cypherQuery = new StringBuilder("CREATE (n:Point {")
          .append(PRIMARY_KEY).append(": ")
          .append("$").append(PRIMARY_KEY).append(",")
          .append(LONGITUDE_COLUMN).append(": ")
          .append("$").append(LONGITUDE_COLUMN).append(",")
          .append(LATITUDE_COLUMN).append(": ")
          .append("$").append(LATITUDE_COLUMN).append(",")
          .append(TIME_OF_RECORD_COLUMN).append(": ")
          .append("$").append(TIME_OF_RECORD_COLUMN)
          .append("})")
          .append("WITH n ")
          .append("CALL spatial.addNode('geom',n) YIELD node ")
          .append("RETURN node");

      Value parameters = Values.parameters(
          PRIMARY_KEY, values.getId(),
          LONGITUDE_COLUMN, values.getLongitude(),
          LATITUDE_COLUMN, values.getLatitude(),
          TIME_OF_RECORD_COLUMN, values.getTimeOfRecord().getTime());

      tx.run(cypherQuery.toString(), parameters);
      tx.commit();
      return Status.OK;
    } catch (Exception e) {
      if (tx != null && tx.isOpen()) {
        try {
          tx.rollback();
        } catch (Exception rollbackException) {
          System.err.println("Failed to rollback transaction: " + rollbackException.getMessage());
        }
      }
      System.err.println("Failed to insert data: " + e.getMessage());
      return Status.ERROR;
    } finally {
      insertLock.unlock();
    }
  }

  @Override
  public Status scanKNN(String table, double lat, double lgn, int k, ArrayList<GeoPoint> results) {

    Transaction tx = null;
    try {
      Session session = sessionHolder.get();
      tx = session.beginTransaction();

      knnLock.lock();

      StringBuilder cypherQuery = new StringBuilder("MATCH (p:Point) ")
          .append("WHERE p.id IS NOT NULL AND p.time_of_record IS NOT NULL RETURN ")
          .append("p.").append(PRIMARY_KEY).append(",")
          .append("p.").append(LATITUDE_COLUMN).append(",")
          .append("p.").append(LONGITUDE_COLUMN).append(",")
          .append("p.").append(TIME_OF_RECORD_COLUMN)
          .append(" ORDER BY point.distance(")
          .append("point({latitude: ")
          .append("p.").append(LATITUDE_COLUMN).append(",")
          .append("longitude: ")
          .append("p.").append(LONGITUDE_COLUMN).append("}),")
          .append("point({latitude: ")
          .append("$").append(LATITUDE_COLUMN).append(",")
          .append("longitude: ")
          .append("$").append(LONGITUDE_COLUMN).append("})) ")
          .append("LIMIT $k");

      Value parameters = Values.parameters(LATITUDE_COLUMN, lat, LONGITUDE_COLUMN, lgn, "k", k);

      Result result = tx.run(cypherQuery.toString(), parameters);

      List<Record> records = result.list();

      for (Record record : records) {
        String id = record.get("p." + PRIMARY_KEY).asString();
        double latitude = record.get("p." + LATITUDE_COLUMN).asDouble();
        double longitude = record.get("p." + LONGITUDE_COLUMN).asDouble();
        long time = record.get("p." + TIME_OF_RECORD_COLUMN).asLong();
        Date date = new Date(time);

        results.add(new GeoPoint(id, latitude, longitude, date));
      }

      tx.commit();
      return Status.OK;
    } catch (Exception e) {
      if (tx != null && tx.isOpen()) {
        try {
          tx.rollback();
        } catch (Exception rollbackException) {
          System.err.println("Failed to rollback transaction: " + rollbackException.getMessage());
        }
      }
      System.err.println("Error occurred during the execution of the KNN query: " + e.getMessage());
      return Status.ERROR;

    } finally {
      knnLock.unlock();
    }
  }

  @Override
  public Status scanByDistance(String table, double lat, double lgn, double maxDistance, ArrayList<GeoPoint> results) {
    Transaction tx = null;
    try {
      Session session = sessionHolder.get();
      tx = session.beginTransaction();

      double maxDistanceInKM = maxDistance * 1000;

      distanceLock.lock();

      StringBuilder cypherQuery = new StringBuilder("MATCH (p:Point) ")
          .append("WHERE p.id IS NOT NULL AND p.time_of_record IS NOT NULL AND ")
          .append("point.distance(")
          .append("point({latitude: ")
          .append("p.").append(LATITUDE_COLUMN).append(",")
          .append("longitude: ")
          .append("p.").append(LONGITUDE_COLUMN).append("}),")
          .append("point({latitude: ")
          .append("$").append(LATITUDE_COLUMN).append(",")
          .append("longitude: ")
          .append("$").append(LONGITUDE_COLUMN).append("})) ")
          .append("<= $maxDistance ")
          .append("RETURN ")
          .append("p.").append(PRIMARY_KEY).append(",")
          .append("p.").append(LATITUDE_COLUMN).append(",")
          .append("p.").append(LONGITUDE_COLUMN).append(",")
          .append("p.").append(TIME_OF_RECORD_COLUMN);

      Value parameters = Values.parameters(LATITUDE_COLUMN, lat, LONGITUDE_COLUMN, lgn, "maxDistance",
          maxDistanceInKM);

      Result result = tx.run(cypherQuery.toString(), parameters);

      // Processamento dos resultados
      while (result.hasNext()) {
        Record record = result.next();
        String id = record.get("p." + PRIMARY_KEY).asString();
        double latitude = record.get("p." + LATITUDE_COLUMN).asDouble();
        double longitude = record.get("p." + LONGITUDE_COLUMN).asDouble();
        long time = record.get("p." + TIME_OF_RECORD_COLUMN).asLong();
        Date date = new Date(time);

        results.add(new GeoPoint(id, latitude, longitude, date));
      }

      tx.commit();

      return Status.OK;
    } catch (Exception e) {
      if (tx != null && tx.isOpen()) {
        try {
          tx.rollback();
        } catch (Exception rollbackException) {
          System.err.println("Failed to rollback transaction: " + rollbackException.getMessage());
        }
      }
      System.err.println("Error occurred during the execution of the Distance query: " + e.getMessage());
      return Status.ERROR;
    } finally {
      distanceLock.unlock();
    }
  }

  @Override
  public Status scanByPolygon(String table, ArrayList<GeoPoint> polygonVertices, ArrayList<GeoPoint> results) {
    Transaction tx = null;
    try {
      Session session = sessionHolder.get();
      tx = session.beginTransaction();

      List<String> coodenateList = polygonVertices.stream()
          .map((GeoPoint point) -> point.getLongitude() + " " + point.getLatitude())
          .collect(Collectors.toList());

      String coodenates = String.join(",", coodenateList);

      polygonLock.lock();

      StringBuilder cypherQuery = new StringBuilder("WITH 'POLYGON((")
          .append(coodenates).append("))' as polygon ")
          .append("CALL spatial.intersects('geom',polygon) YIELD node AS p ")
          .append("WHERE p.id IS NOT NULL AND p.time_of_record IS NOT NULL RETURN ")
          .append("p.").append(PRIMARY_KEY).append(",")
          .append("p.").append(LATITUDE_COLUMN).append(",")
          .append("p.").append(LONGITUDE_COLUMN).append(",")
          .append("p.").append(TIME_OF_RECORD_COLUMN);

      Result result = tx.run(cypherQuery.toString());

      while (result.hasNext()) {
        Record record = result.next();

        String id = record.get("p." + PRIMARY_KEY).asString();
        double latitude = record.get("p." + LATITUDE_COLUMN).asDouble();
        double longitude = record.get("p." + LONGITUDE_COLUMN).asDouble();
        long time = record.get("p." + TIME_OF_RECORD_COLUMN).asLong();
        Date date = new Date(time);

        results.add(new GeoPoint(id, latitude, longitude, date));
      }

      tx.commit();
      return Status.OK;

    } catch (Exception e) {
      if (tx != null && tx.isOpen()) {
        try {
          tx.rollback();
        } catch (Exception rollbackException) {
          System.err.println("Failed to rollback transaction: " + rollbackException.getMessage());
        }
      }

      System.err.println("Error occurred during the execution of the polygon query: " + e.getMessage());
      return Status.ERROR;

    } finally {
      polygonLock.unlock();
    }
  }

  @Override
  public Status read(String label, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.ERROR;
  }

  @Override
  public Status scan(String label, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

  @Override
  public Status update(String label, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status insert(String label, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status delete(String label, String key) {
    return Status.ERROR;
  }

}
