import java.io.FileNotFoundException
import java.sql.{Connection, DriverManager, Statement}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

  trait DataHadoop extends App {

    val conf = new Configuration()
    conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/Users/ishrathnayeem/Hadoop/opt/hadoop/etc/cloudera/hdfs-site.xml"))
    val hadoop = FileSystem.get(conf)

    val stagingarea = new Path("/user/fall2019/ishrath/project4")

    val trips = new Path("/user/fall2019/ishrath/project4/trips")
    val calendar_dates = new Path("/user/fall2019/ishrath/project4/calendar_dates")
    val frequencies = new Path("/user/fall2019/ishrath/project4/frequencies")

    val trips_txt = new Path("/Users/ishrathnayeem/IntelliJ IDEA/mcit/gtfs_stm/trips.txt")
    val calendar_dates_txt = new Path("/Users/ishrathnayeem/IntelliJ IDEA/mcit/gtfs_stm/calendar_dates.txt")
    val frequencies_txt = new Path("/Users/ishrathnayeem/IntelliJ IDEA/mcit/gtfs_stm/frequencies.txt")

    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName)
    val connection: Connection = DriverManager
      .getConnection("jdbc:hive2://quickstart.cloudera:10000/fall2019_ishrath;user=ishrathnayeem;password=Faheemnayeem1.")
    val stmt: Statement = connection.createStatement()

    def Staging() {
      if (hadoop.exists(stagingarea))
        try {
          hadoop.delete(stagingarea, true)
          hadoop.mkdirs(stagingarea)
          hadoop.listStatus(stagingarea)
          println("\nPROJECT4 Directory was CREATED\n")
        }
        catch {
          case _: FileNotFoundException =>
            println("PROJECT4 PATH CANNOT FIND\n")
        }

      hadoop.mkdirs(trips)
      println("Created directory for trips")

      hadoop.mkdirs(calendar_dates)
      println("Created directory for calendar_dates")

      hadoop.mkdirs(frequencies)
      println("Created directory for frequencies\n")
    }

    def CopyFiles(): Unit = {
      if (hadoop.exists(stagingarea)) {
        hadoop.copyFromLocalFile(trips_txt, trips)
        println("trips.txt is copied to the path")
      }
      else println("project4 not found")
      if (hadoop.exists(calendar_dates)) {
        hadoop.copyFromLocalFile(calendar_dates_txt, calendar_dates)
        println("calendar_dates.txt is copied to the path")
      }
      else println("path of calendar_dates not found")
      if (hadoop.exists(frequencies)) {
        hadoop.copyFromLocalFile(frequencies_txt, frequencies)
        println("frequencies.txt is copied to the path\n")
      }
      else println("path not found\n")
    }
  }
