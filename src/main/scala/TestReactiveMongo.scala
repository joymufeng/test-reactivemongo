import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}
import reactivemongo.bson.BSONDocument

import scala.concurrent.{ExecutionContext, Future}

object TestReactiveMongo {
  // My settings (see available connection options)
  val mongoUri = "mongodb://test:test@106.14.154.101:27117/test?authMode=scram-sha1"

  import ExecutionContext.Implicits.global // use any appropriate context

  // Connect to the database: Must be done only once per application
  val driver = MongoDriver()
  val parsedUri = MongoConnection.parseURI(mongoUri)
  val connection = parsedUri.map(driver.connection(_))

  // Database and collections: Get references
  val futureConnection = Future.fromTry(connection)
  def db: Future[DefaultDB] = futureConnection.flatMap(_.database("test"))
  def statSessionColFuture: Future[BSONCollection] = db.map(_.collection("stat-session"))

  def main(args: Array[String]): Unit = {
    println("start ...")
    println(LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
    val startTime = System.currentTimeMillis()
    while (true) {
      val robotId = System.currentTimeMillis().toString
      val channel = System.currentTimeMillis().toString
      val userId = System.currentTimeMillis().toString
      val sessionId = System.currentTimeMillis().toString

      statSessionColFuture.flatMap(_.update(
        BSONDocument("robotId" -> robotId, "channel" -> channel, "userId" -> userId, "sessionId" -> sessionId),
        BSONDocument(
          "$inc" -> BSONDocument("chatCount" -> 1),
          "$set" -> BSONDocument("updateTime" -> System.currentTimeMillis()),
          "$setOnInsert" -> BSONDocument("createTime" -> System.currentTimeMillis())
        ),
        upsert = true
      ))

      Thread.sleep(10)

      // Record time every 30s
      if ((System.currentTimeMillis() - startTime) / 10 % 3000 == 0) {
        println(LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
      }
    }
  }
}