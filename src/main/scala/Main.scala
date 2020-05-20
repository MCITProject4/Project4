object Main extends App with DataHadoop {
  if (hadoop.exists(stagingarea)) {
    hadoop.copyFromLocalFile(trips_txt,trips)
}
  else{
    println("project4 not found")
  }
}
