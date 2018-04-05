package org.yoda.es

import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.yoda.Commons._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ESClientSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  import system.dispatcher

  private var binding: ServerBinding = _
  private val Timeout = 5 seconds
  private val Port = 9090
  private val esResp = """{"took": 182,"errors": false,"items": [{"index": {"_index": "slowlog","_type": "log","_id": "8MvWkWIBZyRh7L_ozGd9","_version": 1,"result": "created","_shards": {"total": 2,"successful": 1,"failed": 0},"_seq_no": 78438,"_primary_term": 2,"status": 201}},{"index": {"_index": "slowlog","_type": "log","_id": "8cvWkWIBZyRh7L_ozGd9","_version": 1,"result": "created","_shards": {"total": 2,"successful": 1,"failed": 0},"_seq_no": 79113,"_primary_term": 2,"status": 201}}]}""".stripMargin

  override def beforeAll(): Unit = {
    binding = Await.result(createHTTPServer(Port), Timeout)
    Thread.sleep(3000)
  }

  val url = s"http://localhost:$Port"
  "ESInsert" should {
    "send es request and parse response" in {
      Source.fromIterator(() => List(Map("a" -> "b"), Map("b" -> "c")).toIterator)
        .via(ESClient.insertData(s"http://localhost:$Port"))
        .runWith(TestSink.probe[Seq[Long]])
        .request(1)
        .expectNext() shouldBe List(201l, 201l)

      Thread.sleep(1000)
    }
  }

  private def createHTTPServer(port: Int): Future[ServerBinding] =
    Http(system = system)
      .bind(interface = "localhost", port = port)
      .to(Sink.foreach(_.handleWithSyncHandler(_ => HttpResponse(StatusCodes.OK, entity = HttpEntity(esResp))))).run()

  override def afterAll(): Unit = Await.result(binding.unbind(), Timeout)
}
