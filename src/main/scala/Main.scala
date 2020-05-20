
object Main extends  DataHadoop  {
  var create = new ExtTables
  create.Staging()
  create.CreateTables()
  create.ManagedTable()
  create.CopyFiles()

  println("!!! ENRICHMENT IN PROGRESS !!!\n\n")

  stmt execute """SET hive.exec.dynamic.partition.mode=nonstrict"""
  stmt.executeUpdate(
    """INSERT OVERWRITE TABLE fall2019_ishrath.enriched_trip PARTITION (wheelchair_accessible)
      |SELECT t.route_id,t.service_id,t.trip_id,t.trip_headsign,t.direction_id,t.shape_id,
      |t.note_fr,t.note_en,f.start_time,f.end_time,f.headway_secs,c.date,c.exception_type,t.wheelchair_accessible
      |FROM ext_trips t
      |FULL OUTER JOIN fall2019_ishrath.ext_calendar_dates c
      |ON t.service_id = c.service_id
      |FULL OUTER JOIN fall2019_ishrath.ext_frequencies f
      |ON t.trip_id = f.trip_id""".stripMargin)

  println("<---ENRICHMENT DONE--->")

  stmt.close()
  connection.close()
}


