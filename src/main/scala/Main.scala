
object Main extends ExtTables with DataHadoop {
  if (hadoop.exists(stagingarea)) {
    hadoop.copyFromLocalFile(trips_txt,trips)
    println("trips.txt is copied to the path")

}
  else println("project4 not found")

  if (hadoop.exists(calendar_dates))
  {
    hadoop.copyFromLocalFile(calendar_dates_txt, calendar_dates)
    println("calendar_dates.txt is copied to the path")}
  else println("path of calendar_dates not found")

if (hadoop.exists(frequencies)){
  hadoop.copyFromLocalFile(frequencies_txt,frequencies)
  println("frequencies.txt file added")
}
else println("path not found")
  stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

  stmt.executeUpdate(
    """INSERT OVERWRITE TABLE fall2019_ishrath.enriched_trip PARTITION (wheelchair_accessible)
      |SELECT t.route_id,t.service_id,t.trip_id,t.trip_headsign,t.direction_id,t.shape_id,
      |t.note_fr,t.note_en,f.start_time,f.end_time,f.headway_secs,c.date,c.exception_type,t.wheelchair_accessible
      |FROM ext_trips t
      |FULL OUTER JOIN fall2019_ishrath.ext_calendar_dates c
      |ON t.service_id = c.service_id
      |FULL OUTER JOIN fall2019_ishrath.ext_frequencies f
      |ON t.trip_id = f.trip_id""".stripMargin)

}


