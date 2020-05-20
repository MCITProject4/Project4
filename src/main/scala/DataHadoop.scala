import java.io.FileNotFoundException
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait DataHadoop extends App {

    val conf = new Configuration()

    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
    val hadoop = FileSystem.get(conf)

    val stagingarea = new Path("/user/fall2019/srujan/project4")

<<<<<<< HEAD
    val trips = new Path("/user/fall2019/ishrath/project4/trips")
    val calendar_dates = new Path("/user/fall2019/ishrath/project4/calendar_dates")
    val frequencies = new Path("/user/fall2019/ishrath/project4/frequencies")
=======
    val trips = new Path("/user/fall2019/srujan/project4/trips")
    val calendar_dates = new Path("/user/fall2019/srujan/project4/calendar_dates")
    val  frequencies = new Path("/user/fall2019/srujan/project4/frequencies")
>>>>>>> 2bc3f6cc7b5bb99b591e0bf6194d9130348d1314

    val trips_txt = new Path("/home/bd-user/Downloads/trips.txt")
    val calendar_dates_txt = new Path("/home/bd-user/Downloads/calendar_dates.txt")
    val frequencies_txt = new Path("/home/bd-user/Downloads/frequencies.txt")

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
<<<<<<< HEAD
    println("created directory for frequencies\n")
  }
=======
    println("created directory for frequencies")
}
>>>>>>> 2bc3f6cc7b5bb99b591e0bf6194d9130348d1314
