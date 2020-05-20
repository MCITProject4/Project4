
object Main extends  DataHadoop  {

  var create = new ExtTables
  create.Staging()
  create.CreateTables()
  create.CopyFiles()
  create.ManagedTable()


  stmt.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

  stmt.execute("INSERT OVERWRITE TABLE fall2019_snehith.enriched_trip PARTITION(wheelchair_accessible) "+
    " SELECT t.route_id,t.service_id,t.trip_id,t.trip_headsign," +
    "t.direction_id,t.shape_id,t.note_fr,t.note_en,f.start_time,f.end_time,f.headway_secs,c.date,c.exception_type,t.wheelchair_accessible "+
    " FROM fall2019_snehith.ext_trips AS t "+
    "FULL OUTER JOIN fall2019_snehith.ext_frequencies As f "+
    " ON t.trip_id = f.trip_id "+
    " FULL OUTER JOIN fall2019_snehith.ext_calendar_dates AS c "+
    " ON t.service_id =  c.service_id " +
    " WHERE t.wheelchair_accessible < 5 ")
  println("inserted data")
  stmt.close()
  connection.close()

}


