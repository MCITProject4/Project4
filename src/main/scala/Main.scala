
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
  stmt.execute("INSERT OVERWRITE TABLE enriched_trip " +
    "PARTITION(wheelchair_accessible) " +
    "SELECT t1.route_id,t1.service_id,t1.trip_id,t1.trip_headsign,t1.direction_id," +
    "t1.shape_id,t1.note_fr,t1.note_en,c1.`date`,c1.exception_type," +
    "f1.start_time,f1.end_time,f1.headway_secs ,t1.wheelchair_accessible" +
    " FROM ext_trips t1 " +
    "LEFT JOIN ext_frequencies f1 " +
    "ON t1.trip_id=f1.trip_id " +
    "LEFT JOIN ext_calendar_dates c1" +
    " ON c1.service_id=t1.service_id")
  println("data is inserted in to enriched table")


}


