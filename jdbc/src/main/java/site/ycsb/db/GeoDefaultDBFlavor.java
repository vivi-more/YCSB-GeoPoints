package site.ycsb.db;

public class GeoDefaultDBFlavor {
    public String createScanKNNStatement(String table) {
        StringBuilder scanKNN = new StringBuilder("SELECT * FROM ");
        scanKNN.append(table);
        scanKNN.append(" ORDER BY ST_SetSRID(ST_MakePoint(");
        scanKNN.append(JdbcDBGeoClient.LONGITUDE_COLUMN);
        scanKNN.append(",");
        scanKNN.append(JdbcDBGeoClient.LATITUDE_COLUMN);
        scanKNN.append("), ");
        scanKNN.append(JdbcDBGeoClient.SRID);
        scanKNN.append(") ");
        scanKNN.append(" <-> ST_SetSRID(ST_MakePoint(?, ?), ");
        scanKNN.append(JdbcDBGeoClient.SRID);
        scanKNN.append(") ");
        scanKNN.append(" LIMIT ?");
        return scanKNN.toString();
    }

    public String createScanDistanceStatement(String table) {
        StringBuilder scanDistance = new StringBuilder("SELECT * FROM ");
        scanDistance.append(table);
        scanDistance.append(" WHERE ST_DWithin(ST_SetSRID(ST_MakePoint(");
        scanDistance.append(JdbcDBGeoClient.LONGITUDE_COLUMN);
        scanDistance.append(", ");
        scanDistance.append(JdbcDBGeoClient.LATITUDE_COLUMN);
        scanDistance.append("), ");
        scanDistance.append(JdbcDBGeoClient.SRID);
        scanDistance.append("), ST_SetSRID(ST_MakePoint(?, ?), ");
        scanDistance.append(JdbcDBGeoClient.SRID);
        scanDistance.append("), ?)");
        return scanDistance.toString();
    }

    public String createScanPolygonStatement(String table) {
        StringBuilder scanPolygon = new StringBuilder("SELECT * FROM ");
        scanPolygon.append(table);
        scanPolygon.append(" WHERE ST_Within(ST_SetSRID(ST_MakePoint(");
        scanPolygon.append(JdbcDBGeoClient.LONGITUDE_COLUMN);
        scanPolygon.append(", ");
        scanPolygon.append(JdbcDBGeoClient.LATITUDE_COLUMN);
        scanPolygon.append("), ");
        scanPolygon.append(JdbcDBGeoClient.SRID);
        scanPolygon.append("), ST_GeomFromText(?, ");
        scanPolygon.append(JdbcDBGeoClient.SRID);
        scanPolygon.append("))");
        return scanPolygon.toString();
    }

    public String createInsertStatement(String table) {
        StringBuilder insert = new StringBuilder("INSERT INTO ")
                .append(table)
                .append(" (")
                .append(JdbcDBGeoClient.PRIMARY_KEY).append(",")
                .append(JdbcDBGeoClient.LONGITUDE_COLUMN).append(",")
                .append(JdbcDBGeoClient.LATITUDE_COLUMN).append(",")
                .append(JdbcDBGeoClient.TIME_OF_RECORD_COLUMN).append(")")
                .append(" VALUES(?,?,?,?)");
        return insert.toString();
    }
}
