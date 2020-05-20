import java.sql.{Connection, DriverManager, Statement}

class ExtTables {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager
    .getConnection("jdbc:hive2://172.16.129.58:10000/snehith;user=snehith;password=snehith")
  val stmt: Statement = connection.createStatement()
  stmt.execute("DROP TABLE IF EXISTS fall2019_snehith.ext_trips")

  stmt.execute("CREATE EXTERNAL TABLE fall2019_snehith.ext_trips ( " +
    "route_id INT,"+
    "service_id STRING,"+
    "trip_id STRING,"+
    "trip_headsign STRING,"+
    "direction_id INT,"+
    "shape_id INT,"+
    "wheelchair_accessible INT,"+
    "note_fr STRING,"+
    "note_en STRING ) " +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    " STORED AS TEXTFILE "+
    " LOCATION '/user/fall2019/snehith/project4/trips'"+
    " tblproperties(" +
    "'skip.header.line.count' = '1',"+
    "'serialization.null.format' = '')"

  )
}
