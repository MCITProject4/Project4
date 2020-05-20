import java.io.FileNotFoundException
import java.sql.{Connection, DriverManager, Statement}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

  trait DataHadoop extends App {

    val conf = new Configuration()
    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
    val hadoop = FileSystem.get(conf)

    val stagingarea = new Path("/user/fall2019/snehith/project4")

    val trips = new Path("/user/fall2019/snehith/project4/trips")
    val calendar_dates = new Path("/user/fall2019/snehith/project4/calendar_dates")
    val frequencies = new Path("/user/fall2019/snehith/project4/frequencies")

    val trips_txt = new Path("/home/snehith/Documents/stm/trips.txt")
    val calendar_dates_txt = new Path("/home/snehith/Documents/stm/calendar_dates.txt")
    val frequencies_txt = new Path("/home/snehith/Documents/stm/frequencies.txt")

    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName)
    val connection: Connection = DriverManager
      .getConnection("jdbc:hive2://172.16.129.58:10000/snehith;user=snehith;password=snehith")
    val stmt: Statement = connection.createStatement()

    def Staging() {
      if (hadoop.exists(stagingarea))
        try {
          hadoop.delete(stagingarea, true)
          hadoop.mkdirs(stagingarea)
          hadoop.listStatus(stagingarea)
          println("PROJECT4 Directory was CREATED\n")
        }
        catch {
          case _: FileNotFoundException =>
            println("PROJECT4 PATH CANNOT FIND\n")
        }
      hadoop.mkdirs(trips)
      println("created directory for trips")

      hadoop.mkdirs(calendar_dates)
      println("created directory for calendar_dates")

      hadoop.mkdirs(frequencies)
      println("created directory for frequencies")
    }
    def CopyFiles(): Unit =
    {
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
    }
  }
