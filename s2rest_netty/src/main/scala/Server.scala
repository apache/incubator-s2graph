package com.kakao.s2graph.rest.netty

import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.types.{LabelWithDirection, VertexId}
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config.ConfigFactory
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.util.CharsetUtil
import play.api.libs.json._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class S2RestHandler extends SimpleChannelInboundHandler[FullHttpRequest] with JSONParser {

  import scala.concurrent.ExecutionContext.Implicits.global

  val CONTENT_TYPE = "Content-Type"
  val CONTENT_LENGTH = "Content-Length"
  val CONNECTION = "Connection"
  val JSON = "application/json"
  val version: ByteBuf = Unpooled.copiedBuffer("with netty", CharsetUtil.UTF_8)
  val Ok = HttpResponseStatus.OK
  val Close = ChannelFutureListener.CLOSE
  val BadRequest = HttpResponseStatus.BAD_REQUEST
  val BadGateway = HttpResponseStatus.BAD_GATEWAY
  val NotFound = HttpResponseStatus.NOT_FOUND

  val InternalServerError = HttpResponseStatus.INTERNAL_SERVER_ERROR
  val s2 = NettyServer.s2graph
  val s2Parser = NettyServer.s2parser

  def badRoute(ctx: ChannelHandlerContext) = simpleResponse(ctx, BadGateway, byteBufOpt = None, channelFutureListenerOpt = Option(Close))

  def simpleResponse(ctx: ChannelHandlerContext,
                     httpResponseStatus: HttpResponseStatus,
                     byteBufOpt: Option[ByteBuf] = None,
                     headers: Seq[(String, Any)] = Nil,
                     channelFutureListenerOpt: Option[ChannelFutureListener] = None): Unit = {
    val res: FullHttpResponse = byteBufOpt match {
      case None => new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, httpResponseStatus)
      case Some(byteBuf) =>
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, httpResponseStatus, byteBuf)
    }
    headers.foreach { case (k, v) => res.headers().set(k, v) }
    val channelFuture = ctx.writeAndFlush(res)
    channelFutureListenerOpt match {
      case None =>
      case Some(listener) => channelFuture.addListener(listener)
    }
  }

  def checkEdgesInner(jsValue: JsValue) = {
    val params = jsValue.as[List[JsValue]]
    var isReverted = false
    val labelWithDirs = scala.collection.mutable.HashSet[LabelWithDirection]()
    val quads = for {
      param <- params
      labelName <- (param \ "label").asOpt[String]
      direction <- GraphUtil.toDir((param \ "direction").asOpt[String].getOrElse("out"))
      label <- Label.findByName(labelName)
      srcId <- jsValueToInnerVal((param \ "from").as[JsValue], label.srcColumnWithDir(direction.toInt).columnType, label.schemaVersion)
      tgtId <- jsValueToInnerVal((param \ "to").as[JsValue], label.tgtColumnWithDir(direction.toInt).columnType, label.schemaVersion)
    } yield {
      val labelWithDir = LabelWithDirection(label.id.get, direction)
      labelWithDirs += labelWithDir
      val (src, tgt, dir) = if (direction == 1) {
        isReverted = true
        (Vertex(VertexId(label.tgtColumnWithDir(direction.toInt).id.get, tgtId)),
          Vertex(VertexId(label.srcColumnWithDir(direction.toInt).id.get, srcId)), 0)
      } else {
        (Vertex(VertexId(label.srcColumnWithDir(direction.toInt).id.get, srcId)),
          Vertex(VertexId(label.tgtColumnWithDir(direction.toInt).id.get, tgtId)), 0)
      }
      (src, tgt, QueryParam(LabelWithDirection(label.id.get, dir)))
    }

    s2.checkEdges(quads).map { case queryRequestWithResultLs =>
      val edgeJsons = for {
        queryRequestWithResult <- queryRequestWithResultLs
        (queryRequest, queryResult) = QueryRequestWithResult.unapply(queryRequestWithResult).get
        edgeWithScore <- queryResult.edgeWithScoreLs
        (edge, score) = EdgeWithScore.unapply(edgeWithScore).get
        convertedEdge = if (isReverted) edge.duplicateEdge else edge
        edgeJson = PostProcess.edgeToJson(convertedEdge, score, queryRequest.query, queryRequest.queryParam)
      } yield Json.toJson(edgeJson)

      Json.toJson(edgeJsons)
    }
  }

  private def eachQuery(post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue)(q: Query): Future[JsValue] = {
    val filterOutQueryResultsLs = q.filterOutQuery match {
      case Some(filterOutQuery) => s2.getEdges(filterOutQuery)
      case None => Future.successful(Seq.empty)
    }

    for {
      queryResultsLs <- s2.getEdges(q)
      filterOutResultsLs <- filterOutQueryResultsLs
    } yield {
      val json = post(queryResultsLs, filterOutResultsLs)
      json
    }
  }

  private def calcSize(js: JsValue): Int = js match {
    case JsObject(obj) => (js \ "size").asOpt[Int].getOrElse(0)
    case JsArray(seq) => seq.map(js => (js \ "size").asOpt[Int].getOrElse(0)).sum
    case _ => 0
  }

  def jsonResponse(ctx: ChannelHandlerContext, json: JsValue, headers: (String, String)*) = {
    val byteBuf = Unpooled.copiedBuffer(json.toString, CharsetUtil.UTF_8)
    if (NettyServer.isHealthy)
      simpleResponse(ctx, Ok, byteBufOpt = Option(byteBuf), headers = headers.toSeq)
    else
      simpleResponse(ctx, Ok, byteBufOpt = Option(byteBuf), headers = headers.toSeq, channelFutureListenerOpt = Option(Close))
  }

  private def getEdgesAsync(jsonQuery: JsValue)
                           (post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue): Future[JsValue] = {
    val fetch = eachQuery(post) _
    //    logger.info(jsonQuery)
    jsonQuery match {
      case JsArray(arr) => Future.traverse(arr.map(s2Parser.toQuery(_)))(fetch).map(JsArray)
      case obj@JsObject(_) => fetch(s2Parser.toQuery(obj))
      case _ => throw BadQueryException("Cannot support")
    }
  }

  private def getEdgesExcludedAsync(jsonQuery: JsValue)
                                   (post: (Seq[QueryRequestWithResult], Seq[QueryRequestWithResult]) => JsValue): Future[JsValue] = {
    val q = s2Parser.toQuery(jsonQuery)
    val filterOutQuery = Query(q.vertices, Vector(q.steps.last))

    val fetchFuture = s2.getEdges(q)
    val excludeFuture = s2.getEdges(filterOutQuery)

    for {
      queryResultLs <- fetchFuture
      exclude <- excludeFuture
    } yield {
      post(queryResultLs, exclude)
    }
  }

  private def getVerticesInner(jsValue: JsValue) = {
    val jsonQuery = jsValue
    val ts = System.currentTimeMillis()
    val props = "{}"

    val vertices = jsonQuery.as[List[JsValue]].flatMap { js =>
      val serviceName = (js \ "serviceName").as[String]
      val columnName = (js \ "columnName").as[String]
      for (id <- (js \ "ids").asOpt[List[JsValue]].getOrElse(List.empty[JsValue])) yield {
        Management.toVertex(ts, "insert", id.toString, serviceName, columnName, props)
      }
    }

    s2.getVertices(vertices) map { vertices => PostProcess.verticesToJson(vertices) }
  }

  private def experiment(request: FullHttpRequest, accessToken: String, experimentName: String, uuid: String) = {
    val bucketOpt = for {
      service <- Service.findByAccessToken(accessToken)
      experiment <- Experiment.findBy(service.id.get, experimentName)
      bucket <- experiment.findBucket(uuid)
    } yield bucket

    val bucket = bucketOpt.getOrElse(throw new RuntimeException("bucket is not found"))
    if (bucket.isGraphQuery) buildRequestInner(request, bucket, uuid)
    else throw new RuntimeException("not supported yet")
    //    else buildRequest(request, bucket, uuid)
  }

  def makeRequestJson(requestKeyJsonOpt: Option[JsValue], bucket: Bucket, uuid: String): JsValue = {
    var body = bucket.requestBody.replace("#uuid", uuid)

    for {
      requestKeyJson <- requestKeyJsonOpt
      jsObj <- requestKeyJson.asOpt[JsObject]
      (key, value) <- jsObj.fieldSet
    } {
      val replacement = value match {
        case JsString(s) => s
        case _ => value.toString
      }
      body = body.replace(key, replacement)
    }

    Json.parse(body)
  }

  private def buildRequestInner(request: FullHttpRequest, bucket: Bucket, uuid: String): Future[JsValue] = {
    if (bucket.isEmpty) Future.successful(Json.obj("isEmpty" -> true))
    else {
      val jsonString = request.content.toString(CharsetUtil.UTF_8)
      val jsonBody = makeRequestJson(Option(Json.parse(jsonString)), bucket, uuid)
      val path = request.getUri
      // dummy log for sampling
      val experimentLog = s"POST $path took -1 ms 200 -1 $jsonBody"
      logger.info(experimentLog)
      uriMatch(path, jsonBody)
    }
  }

  //
  //  private def toSimpleMap(map: Map[String, Seq[String]]): Map[String, String] = {
  //    for {
  //      (k, vs) <- map
  //      headVal <- vs.headOption
  //    } yield {
  //      k -> headVal
  //    }
  //  }
  //
  //  private def buildRequest(request: FullHttpRequest, bucket: Bucket, uuid: String): Future[JsValue] = {
  //    val jsonString = request.content.toString(CharsetUtil.UTF_8)
  //    val jsonBody = makeRequestJson(Option(Json.parse(jsonString)), bucket, uuid)
  //
  //    val url = bucket.apiPath
  //    val headers = request.headers.toSimpleMap.toSeq
  //    val verb = bucket.httpVerb.toUpperCase
  //    val qs = toSimpleMap(request.queryString).toSeq
  //
  //    val ws = WS.url(url)
  //      .withMethod(verb)
  //      .withBody(jsonBody)
  //      .withHeaders(headers: _*)
  //      .withQueryString(qs: _*)
  //
  //    ws.stream().map {
  //      case (proxyResponse, proxyBody) =>
  //        Result(ResponseHeader(proxyResponse.status, proxyResponse.headers.mapValues(_.toList.head)), proxyBody).withHeaders(impressionKey -> bucket.impressionId)
  //    }
  //  }

  def toResponse(ctx: ChannelHandlerContext, req: FullHttpRequest, jsonQuery: JsValue, future: Future[JsValue], startedAt: Long) = {
    future onComplete {
      case Success(resJson) =>
        val duration = System.currentTimeMillis() - startedAt
        val isKeepAlive = HttpHeaders.isKeepAlive(req)
        val buf: ByteBuf = Unpooled.copiedBuffer(resJson.toString, CharsetUtil.UTF_8)
        val (headers, listenerOpt) =
          if (isKeepAlive) (Seq(CONTENT_TYPE -> Json, CONTENT_LENGTH -> buf.readableBytes(), CONNECTION -> HttpHeaders.Values.KEEP_ALIVE), None)
          else (Seq(CONTENT_TYPE -> Json, CONTENT_LENGTH -> buf.readableBytes()), Option(Close))
        //NOTE: logging size of result should move to s2core.
        //        logger.info(resJson.size.toString)

        val log = s"${req.getMethod} ${req.getUri} took ${duration} ms 200 ${calcSize(resJson)} ${jsonQuery}"
        logger.info(log)

        simpleResponse(ctx, Ok, byteBufOpt = Option(buf), channelFutureListenerOpt = listenerOpt, headers = headers)
      case Failure(ex) => simpleResponse(ctx, InternalServerError, byteBufOpt = None, channelFutureListenerOpt = Option(Close))
    }
  }

  private def uriMatch(uri: String, jsQuery: JsValue): Future[JsValue] = {
    uri match {
      case "/graphs/getEdges" => getEdgesAsync(jsQuery)(PostProcess.toSimpleVertexArrJson)
      case "/graphs/getEdges/grouped" => getEdgesAsync(jsQuery)(PostProcess.summarizeWithListFormatted)
      case "/graphs/getEdgesExcluded" => getEdgesExcludedAsync(jsQuery)(PostProcess.toSimpleVertexArrJson)
      case "/graphs/getEdgesExcluded/grouped" => getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExcludeFormatted)
      case "/graphs/checkEdges" => checkEdgesInner(jsQuery)
      case "/graphs/getEdgesGrouped" => getEdgesAsync(jsQuery)(PostProcess.summarizeWithList)
      case "/graphs/getEdgesGroupedExcluded" => getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExclude)
      case "/graphs/getEdgesGroupedExcludedFormatted" => getEdgesExcludedAsync(jsQuery)(PostProcess.summarizeWithListExcludeFormatted)
      case "/graphs/getVertices" => getVerticesInner(jsQuery)
      case _ => throw new RuntimeException("route is not found")
    }
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.getUri

    req.getMethod match {
      case HttpMethod.GET =>
        uri match {
          case "/health_check.html" => simpleResponse(ctx, Ok, byteBufOpt = None, channelFutureListenerOpt = Option(Close))
          case s if s.startsWith("/graphs/getEdge/") =>
            // src, tgt, label, dir
            val Array(srcId, tgtId, labelName, direction) = s.split("/").takeRight(4)
            val params = Json.arr(Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId))
            val startedAt = System.currentTimeMillis()
            val future = checkEdgesInner(params)
            toResponse(ctx, req, params, future, startedAt)
          case _ => badRoute(ctx)
        }

      case HttpMethod.PUT =>
        if (uri.startsWith("/health_check/")) {
          val newHealthCheck = uri.split("/").last.toBoolean
          val newHealthCheckMsg = Unpooled.copiedBuffer(NettyServer.updateHealthCheck(newHealthCheck).toString, CharsetUtil.UTF_8)
          simpleResponse(ctx, Ok, byteBufOpt = Option(newHealthCheckMsg), channelFutureListenerOpt = Option(Close))
        } else badRoute(ctx)

      case HttpMethod.POST =>
        val jsonString = req.content.toString(CharsetUtil.UTF_8)
        val jsQuery = Json.parse(jsonString)
        //TODO: result_size
        val startedAt = System.currentTimeMillis()

        val future =
          if (uri.startsWith("/graphs/experiment")) {
            val Array(accessToken, experimentName, uuid) = uri.split("/").takeRight(3)
            experiment(req, accessToken, experimentName, uuid)
          } else {
            uriMatch(uri, jsQuery)
          }

        toResponse(ctx, req, jsQuery, future, startedAt)

      case _ =>
        simpleResponse(ctx, BadRequest, byteBufOpt = None, channelFutureListenerOpt = Option(Close))
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    logger.error(s"exception on query.", cause)
    simpleResponse(ctx, BadRequest, byteBufOpt = None, channelFutureListenerOpt = Option(Close))
  }
}

object NettyServer extends App {
  def updateHealthCheck(healthCheck: Boolean): Boolean = {
    this.isHealthy = healthCheck
    this.isHealthy
  }

  val config = ConfigFactory.load()
  val Port = Try(config.getInt("http.port")).recover { case _ => 9000 }.get

  // init s2graph with config
  val s2graph = new Graph(config)(scala.concurrent.ExecutionContext.Implicits.global)
  val s2parser = new RequestParser(s2graph)

  // app status code
  var isHealthy = true

  // Configure the server.
  val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
  val workerGroup: EventLoopGroup = new NioEventLoopGroup()

  try {
    val b: ServerBootstrap = new ServerBootstrap()
    b.option(ChannelOption.SO_BACKLOG, Int.box(2048))

    b.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
      .handler(new LoggingHandler(LogLevel.INFO))
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel) {
          val p = ch.pipeline()
          p.addLast(new HttpServerCodec())
          p.addLast(new HttpObjectAggregator(65536))
          p.addLast(new S2RestHandler())
        }
      })

    val ch: Channel = b.bind(Port).sync().channel()
    ch.closeFuture().sync()

  } finally {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }
}

