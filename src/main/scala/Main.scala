
object Main extends App with DataHadoop {
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
}
