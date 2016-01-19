package com.kakao.s2graph.rest.netty

import java.util.concurrent.Executors

import com.kakao.s2graph.core.GraphExceptions.BadQueryException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.rest.RestHandler.HandlerResult
import com.kakao.s2graph.core.rest._
import com.kakao.s2graph.core.utils.Extensions._
import com.kakao.s2graph.core.utils.logger
import com.typesafe.config.ConfigFactory
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.HttpHeaders._
import io.netty.handler.codec.http._
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.util.CharsetUtil
import play.api.libs.json._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.io.Source
import scala.util.{Failure, Success, Try}

class S2RestHandler(s2rest: RestHandler)(implicit ec: ExecutionContext) extends SimpleChannelInboundHandler[FullHttpRequest] with JSONParser {
  val ApplicationJson = "application/json"

  val Ok = HttpResponseStatus.OK
  val CloseOpt = Option(ChannelFutureListener.CLOSE)
  val BadRequest = HttpResponseStatus.BAD_REQUEST
  val BadGateway = HttpResponseStatus.BAD_GATEWAY
  val NotFound = HttpResponseStatus.NOT_FOUND
  val InternalServerError = HttpResponseStatus.INTERNAL_SERVER_ERROR

  def badRoute(ctx: ChannelHandlerContext) =
    simpleResponse(ctx, BadGateway, byteBufOpt = None, channelFutureListenerOpt = CloseOpt)

  def simpleResponse(ctx: ChannelHandlerContext,
                     httpResponseStatus: HttpResponseStatus,
                     byteBufOpt: Option[ByteBuf] = None,
                     headers: Seq[(String, String)] = Nil,
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

  def toResponse(ctx: ChannelHandlerContext, req: FullHttpRequest, requestBody: JsValue, result: HandlerResult, startedAt: Long) = {
    var closeOpt = CloseOpt
    var headers = mutable.ArrayBuilder.make[(String, String)]

    headers += (Names.CONTENT_TYPE -> ApplicationJson)
    result.headers.foreach(headers += _)

    if (HttpHeaders.isKeepAlive(req)) {
      headers += (Names.CONNECTION -> HttpHeaders.Values.KEEP_ALIVE)
      closeOpt = None
    }

    result.body onComplete {
      case Success(json) =>
        val duration = System.currentTimeMillis() - startedAt

        val log = s"${req.getMethod} ${req.getUri} took ${duration} ms 200 ${s2rest.calcSize(json)} ${requestBody}"
        logger.info(log)

        val buf: ByteBuf = Unpooled.copiedBuffer(json.toString, CharsetUtil.UTF_8)

        headers += (Names.CONTENT_LENGTH -> buf.readableBytes().toString)

        simpleResponse(ctx, Ok, byteBufOpt = Option(buf), channelFutureListenerOpt = closeOpt, headers = headers.result())
      case Failure(ex) => ex match {
        case e: BadQueryException =>
          logger.error(s"{$requestBody}, ${e.getMessage}", e)
          val buf: ByteBuf = Unpooled.copiedBuffer(PostProcess.badRequestResults(e).toString, CharsetUtil.UTF_8)
          simpleResponse(ctx, Ok, byteBufOpt = Option(buf), channelFutureListenerOpt = closeOpt, headers = headers.result())
        case e: Exception =>
          logger.error(s"${requestBody}, ${e.getMessage}", e)
          val buf: ByteBuf = Unpooled.copiedBuffer(PostProcess.emptyResults.toString, CharsetUtil.UTF_8)
          simpleResponse(ctx, InternalServerError, byteBufOpt = Option(buf), channelFutureListenerOpt = closeOpt, headers = headers.result())
      }
    }
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.getUri
    val startedAt = System.currentTimeMillis()

    req.getMethod match {
      case HttpMethod.GET =>
        uri match {
          case "/health_check.html" =>
            if (NettyServer.isHealthy) {
              val healthCheckMsg = Unpooled.copiedBuffer(NettyServer.deployInfo, CharsetUtil.UTF_8)
              simpleResponse(ctx, Ok, byteBufOpt = Option(healthCheckMsg), channelFutureListenerOpt = CloseOpt)
            } else {
              simpleResponse(ctx, NotFound, channelFutureListenerOpt = CloseOpt)
            }

          case s if s.startsWith("/graphs/getEdge/") =>
            // src, tgt, label, dir
            val Array(srcId, tgtId, labelName, direction) = s.split("/").takeRight(4)
            val params = Json.arr(Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId))
            val result = s2rest.checkEdges(params)
            toResponse(ctx, req, params, result, startedAt)
          case _ => badRoute(ctx)
        }

      case HttpMethod.PUT =>
        if (uri.startsWith("/health_check/")) {
          val newHealthCheck = uri.split("/").last.toBoolean
          NettyServer.isHealthy = newHealthCheck
          val newHealthCheckMsg = Unpooled.copiedBuffer(NettyServer.isHealthy.toString, CharsetUtil.UTF_8)
          simpleResponse(ctx, Ok, byteBufOpt = Option(newHealthCheckMsg), channelFutureListenerOpt = CloseOpt)
        } else badRoute(ctx)

      case HttpMethod.POST =>
        val jsonString = req.content.toString(CharsetUtil.UTF_8)
        val jsQuery = Json.parse(jsonString)

        val result = s2rest.doPost(uri, jsQuery)
        toResponse(ctx, req, jsQuery, result, startedAt)

      case _ =>
        simpleResponse(ctx, BadRequest, byteBufOpt = None, channelFutureListenerOpt = CloseOpt)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    logger.error(s"exception on query.", cause)
    simpleResponse(ctx, BadRequest, byteBufOpt = None, channelFutureListenerOpt = CloseOpt)
  }
}

// Simple http server
object NettyServer extends App {
  /** should be same with Boostrap.onStart on play */

  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread)
  val ec = ExecutionContext.fromExecutor(threadPool)

  val config = ConfigFactory.load()
  val port = Try(config.getInt("http.port")).recover { case _ => 9000 }.get

  // init s2graph with config
  val s2graph = new Graph(config)(ec)
  val rest = new RestHandler(s2graph)(ec)

  val deployInfo = Try(Source.fromFile("./release_info").mkString("")).recover { case _ => "release info not found\n" }.get
  var isHealthy = config.getBooleanWithFallback("app.health.on", true)

  logger.info(s"starts with num of thread: $numOfThread, ${threadPool.getClass.getSimpleName}")

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
          p.addLast(new S2RestHandler(rest)(ec))
        }
      })

    logger.info(s"Listening for HTTP on /0.0.0.0:$port")
    val ch: Channel = b.bind(port).sync().channel()
    ch.closeFuture().sync()

  } finally {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    s2graph.shutdown()
  }
}

