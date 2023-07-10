
package site.ycsb.db;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.geodata.GeoPoint;

public class RedisGeoClient extends DB {

  private JedisCommands jedis;

  public static final String PRIMARY_KEY = "ID";
  public static final String LONGITUDE_COLUMN = "LONGITUDE";
  public static final String LATITUDE_COLUMN = "LATITUDE";
  public static final String TIME_OF_RECORD_COLUMN = "TIME_OF_RECORD";
  public static final String LOCATION_COLUMN = "LOCATION";

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    jedis = new Jedis(host, port);
    ((Jedis) jedis).connect();

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedis).auth(password);
    }

  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  @Override
  public Status reset(String table) {
    try {
    
      ((BasicCommands) jedis).flushDB();
      return Status.OK;

    } catch (JedisException e) {
      // Tratar exceção ao interagir com o Redis
      System.err.println("Error in processing reset of table " + table + ": " + e.getMessage());

      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, GeoPoint gp) {
    try {
      Long qttRecords = jedis.geoadd(LOCATION_COLUMN, gp.getLongitude(), gp.getLatitude(), key);
      if (qttRecords != null && qttRecords >= 1) {

        Long result = jedis.hsetnx(key, TIME_OF_RECORD_COLUMN, GeoUtils.dateToString(gp.getTimeOfRecord()));

        if (result == 1) {
          return Status.OK;
        }
      }

      return Status.ERROR;

    } catch (JedisException e) {
      // Tratar exceção ao interagir com o Redis
      System.err.println("Error in processing insert of table " + table + ": " + e.getMessage());

      return Status.ERROR;
    }
  }

  @Override
  public Status scanKNN(String table, double lat, double lgn, int k, ArrayList<GeoPoint> result) {
    try {
      // Perform the KNN query using georadius with the specified center, radius, and
      // count
      GeoRadiusParam param = GeoRadiusParam.geoRadiusParam().withCoord().sortAscending().count(k);
      GeoCoordinate center = new GeoCoordinate(lgn, lat);

      List<GeoRadiusResponse> results = jedis.georadius(LOCATION_COLUMN, center.getLongitude(),
          center.getLatitude(), Double.MAX_VALUE, GeoUnit.KM, param);

      // Iterar sobre os resultados
      for (GeoRadiusResponse point : results) {
        String id = point.getMemberByString();
        GeoCoordinate gc = point.getCoordinate();

        Date time = GeoUtils.stringtoDate(jedis.hget(id, TIME_OF_RECORD_COLUMN));

        GeoPoint gp = new GeoPoint();

        gp.setId(id);
        gp.setLongitude(gc.getLongitude());
        gp.setLatitude(gc.getLatitude());
        gp.setTimeOfRecord(time);

        result.add(gp);
      }

      return Status.OK;

    } catch (JedisException e) {
      // Tratar exceção ao interagir com o Redis
      System.err.println("Error in processing scan knn of table " + table + ": " + e.getMessage());
    }
    return Status.ERROR;
  }

  @Override
  public Status scanByDistance(String table, double lat, double lgn, double maxDistance,
      ArrayList<GeoPoint> result) {
    try {

      // Perform the KNN query using georadius with the specified center, radius, and
      // count
      GeoRadiusParam param = GeoRadiusParam.geoRadiusParam().withDist().withCoord();
      List<GeoRadiusResponse> results = jedis.georadius(LOCATION_COLUMN, lgn, lat, maxDistance, GeoUnit.KM, param);

      // Iterar sobre os resultados
      for (GeoRadiusResponse point : results) {
        String id = point.getMemberByString();
        GeoCoordinate gc = point.getCoordinate();

        Date time = GeoUtils.stringtoDate(jedis.hget(id, TIME_OF_RECORD_COLUMN));

        GeoPoint gp = new GeoPoint();

        gp.setId(id);
        gp.setLongitude(gc.getLongitude());
        gp.setLatitude(gc.getLatitude());
        gp.setTimeOfRecord(time);

        result.add(gp);
      }

      return Status.OK;

    } catch (JedisException e) {
      // Tratar exceção ao interagir com o Redis
      System.err.println("Error in processing scan distance of table " + table + ": " + e.getMessage());
    }
    return Status.ERROR;
  }

  @Override
  public Status scanByPolygon(String table, ArrayList<GeoPoint> polygonVertices, ArrayList<GeoPoint> result) {
    try {

      double maxDistance = GeoUtils.calculateMaxDistanceInKM(polygonVertices);

      // Obter todos os pontos dentro do polígono usando o comando GEORADIUS
      GeoRadiusParam geoRadiusParam = GeoRadiusParam.geoRadiusParam().withCoord().sortAscending();
      List<GeoRadiusResponse> results = jedis.georadius(LOCATION_COLUMN, polygonVertices.get(0).getLongitude(),
          polygonVertices.get(0).getLatitude(), maxDistance, GeoUnit.KM, geoRadiusParam);

      ArrayList<String> uniqueIdentifiers = new ArrayList<>();

      for (GeoRadiusResponse point : results) {
        String id = point.getMemberByString();

        if (uniqueIdentifiers.contains(id)) {
          continue;
        }

        uniqueIdentifiers.add(id);

        GeoCoordinate gc = point.getCoordinate();

        if (GeoUtils.isCoordinateInsidePolygon(gc.getLongitude(), gc.getLatitude(), polygonVertices)) {

          Date time = GeoUtils.stringtoDate(jedis.hget(id, TIME_OF_RECORD_COLUMN));

          GeoPoint gp = new GeoPoint();

          gp.setId(id);
          gp.setLongitude(gc.getLongitude());
          gp.setLatitude(gc.getLatitude());
          gp.setTimeOfRecord(time);

          result.add(gp);
        }
      }
      return Status.OK;

    } catch (JedisException e) {
      // Tratar exceção ao interagir com o Redis
      System.err.println("Error in processing scan distance of table " + table + ": " + e.getMessage());
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

}
