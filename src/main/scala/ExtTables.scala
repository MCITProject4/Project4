import java.sql.{Connection, DriverManager, Statement}

class ExtTables {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager
    .getConnection("jdbc:hive2://172.16.129.58:10000/snehith;user=snehith;password=snehith")
  val stmt: Statement = connection.createStatement()
  stmt.execute("DROP TABLE IF EXISTS fall2019_snehith.ext_trips")

  stmt.execute("CREATE EXTERNAL TABLE fall2019_snehith.ext_trips ( " +
    "route_id              INT,"+
    "service_id            STRING,"+
    "trip_id               STRING,"+
    "trip_headsign         STRING,"+
    "direction_id          INT,"+
    "shape_id              INT,"+
    "wheelchair_accessible INT,"+
    "note_fr               STRING,"+
    "note_en               STRING ) " +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    " STORED AS TEXTFILE "+
    " LOCATION '/user/fall2019/snehith/project4/trips'"+
    " tblproperties(" +
    "'skip.header.line.count' = '1',"+
    "'serialization.null.format' = '')")

  stmt.execute("DROP TABLE IF EXISTS fall2019_srujan.ext_frequencies")
  stmt.execute("CREATE EXTERNAL TABLE fall2019_srujan.ext_frequencies   (" +
    " trip_id       STRING ," +
    " start_time    STRING, " +
    " end_time      STRING, " +
    " headway_secs  STRING)" +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ','" +
    "STORED AS TEXTFILE" +
    " location '/user/fall2019/srujan/project4/frequencies'" +
    "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')")
  println("frequencies table created")

  stmt execute
    """CREATE EXTERNAL TABLE ext_calendar_dates (
      |service_id       STRING,
      |date             INT,
      |exception_type   INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/fall2019/ishrath/project4/calendar_dates'
      |TBLPROPERTIES (
      |"skip.header.line.count" = "1",
      |"serialization.null.format" = "")""".stripMargin

  println("ext_calendar_dates TABLE was CREATED\n")

  stmt execute """SET hive.exec.dynamic.partition=true"""
  stmt execute """SET hive.exec.dynamic.partition.mode=nonstrict"""

  stmt execute
    """CREATE TABLE fall2019_ishrath.enriched_trip (
      |route_id             STRING,
      |service_id	          STRING,
      |trip_id	            STRING,
      |trip_headsign       	STRING,
      |direction_id        	INT,
      |shape_id	            INT,
      |note_fr             	STRING,
      |note_en             	STRING,
      |start_time	          TIMESTAMP,
      |end_time	            TIMESTAMP,
      |headway_secs        	INT,
      |date	                INT,
      |exception_type      	INT
      |)
      |PARTITIONED BY (wheelchair_accessible int)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS PARQUET""".stripMargin

  println("enriched_trip TABLE was CREATED\n")
}