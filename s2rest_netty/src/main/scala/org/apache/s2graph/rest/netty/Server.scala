/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.rest.netty

import java.util.concurrent.Executors

import com.typesafe.config.ConfigFactory
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http.HttpHeaders._
import io.netty.handler.codec.http._
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.util.CharsetUtil
import org.apache.s2graph.core.GraphExceptions.{BadQueryException}
import org.apache.s2graph.core.schema.Experiment
import org.apache.s2graph.core.rest.RestHandler
import org.apache.s2graph.core.rest.RestHandler.{CanLookup, HandlerResult}
import org.apache.s2graph.core.utils.Extensions._
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{S2Graph, PostProcess}
import play.api.libs.json._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scala.language.existentials

class S2RestHandler(s2rest: RestHandler)(implicit ec: ExecutionContext) extends SimpleChannelInboundHandler[FullHttpRequest] {
  val ApplicationJson = "application/json"

  val Ok = HttpResponseStatus.OK
  val CloseOpt = Option(ChannelFutureListener.CLOSE)
  val BadRequest = HttpResponseStatus.BAD_REQUEST
  val BadGateway = HttpResponseStatus.BAD_GATEWAY
  val NotFound = HttpResponseStatus.NOT_FOUND
  val InternalServerError = HttpResponseStatus.INTERNAL_SERVER_ERROR

  implicit val nettyHeadersLookup = new CanLookup[HttpHeaders] {
    override def lookup(m: HttpHeaders, key: String) = Option(m.get(key))
  }

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

  def toResponse(ctx: ChannelHandlerContext, req: FullHttpRequest, requestBody: String, result: HandlerResult, startedAt: Long) = {
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
        val bucketName = result.headers.toMap.get(Experiment.ImpressionKey).getOrElse("")

        val log = s"${req.getMethod} ${req.getUri} took ${duration} ms 200 ${s2rest.calcSize(json)} ${requestBody} ${bucketName}"
        logger.info(log)

        val buf: ByteBuf = Unpooled.copiedBuffer(json.toString, CharsetUtil.UTF_8)

        headers += (Names.CONTENT_LENGTH -> buf.readableBytes().toString)

        simpleResponse(ctx, Ok, byteBufOpt = Option(buf), channelFutureListenerOpt = closeOpt, headers = headers.result())
      case Failure(ex) => ex match {
        case e: BadQueryException =>
          logger.error(s"{$requestBody}, ${e.getMessage}", e)
          val buf: ByteBuf = Unpooled.copiedBuffer(PostProcess.badRequestResults(e).toString, CharsetUtil.UTF_8)
          simpleResponse(ctx, BadRequest, byteBufOpt = Option(buf), channelFutureListenerOpt = CloseOpt, headers = headers.result())
        case e: Exception =>
          logger.error(s"${requestBody}, ${e.getMessage}", e)
          val buf: ByteBuf = Unpooled.copiedBuffer(PostProcess.emptyResults.toString, CharsetUtil.UTF_8)
          simpleResponse(ctx, InternalServerError, byteBufOpt = Option(buf), channelFutureListenerOpt = CloseOpt, headers = headers.result())
      }
    }
  }

  private def healthCheck(ctx: ChannelHandlerContext)(predicate: Boolean): Unit = {
    if (predicate) {
      val healthCheckMsg = Unpooled.copiedBuffer(NettyServer.deployInfo, CharsetUtil.UTF_8)
      simpleResponse(ctx, Ok, byteBufOpt = Option(healthCheckMsg), channelFutureListenerOpt = CloseOpt)
    } else {
      simpleResponse(ctx, NotFound, channelFutureListenerOpt = CloseOpt)
    }
  }

  private def updateHealthCheck(ctx: ChannelHandlerContext)(newValue: Boolean)(updateOp: Boolean => Unit): Unit = {
    updateOp(newValue)
    val newHealthCheckMsg = Unpooled.copiedBuffer(newValue.toString, CharsetUtil.UTF_8)
    simpleResponse(ctx, Ok, byteBufOpt = Option(newHealthCheckMsg), channelFutureListenerOpt = CloseOpt)
  }

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val uri = req.getUri
    val startedAt = System.currentTimeMillis()
    val checkFunc = healthCheck(ctx) _
    val updateFunc = updateHealthCheck(ctx) _
    req.getMethod match {
      case HttpMethod.GET =>
        uri match {
          case "/health_check.html" => checkFunc(NettyServer.isHealthy)
          case "/fallback_check.html" => checkFunc(NettyServer.isFallbackHealthy)
          case "/query_fallback_check.html" => checkFunc(NettyServer.isQueryFallbackHealthy)
          case s if s.startsWith("/graphs/getEdge/") =>
            if (!NettyServer.isQueryFallbackHealthy) {
              val result = HandlerResult(body = Future.successful(PostProcess.emptyResults))
              toResponse(ctx, req, s, result, startedAt)
            } else {
              val Array(srcId, tgtId, labelName, direction) = s.split("/").takeRight(4)
              val params = Json.arr(Json.obj("label" -> labelName, "direction" -> direction, "from" -> srcId, "to" -> tgtId))
              val result = s2rest.checkEdges(params)
              toResponse(ctx, req, s, result, startedAt)
            }
          case _ => badRoute(ctx)
        }

      case HttpMethod.PUT =>
        if (uri.startsWith("/health_check/")) {
          val newValue = uri.split("/").last.toBoolean
          updateFunc(newValue) { v => NettyServer.isHealthy = v }
        } else if (uri.startsWith("/query_fallback_check/")) {
          val newValue = uri.split("/").last.toBoolean
          updateFunc(newValue) { v => NettyServer.isQueryFallbackHealthy = v }
        } else if (uri.startsWith("/fallback_check/")) {
          val newValue = uri.split("/").last.toBoolean
          updateFunc(newValue) { v => NettyServer.isFallbackHealthy = v }
        } else {
          badRoute(ctx)
        }

      case HttpMethod.POST =>
        val body = req.content.toString(CharsetUtil.UTF_8)
        if (!NettyServer.isQueryFallbackHealthy) {
          val result = HandlerResult(body = Future.successful(PostProcess.emptyResults))
          toResponse(ctx, req, body, result, startedAt)
        } else {
          val result = s2rest.doPost(uri, body, req.headers())
          toResponse(ctx, req, body, result, startedAt)
        }

      case _ =>
        simpleResponse(ctx, BadRequest, byteBufOpt = None, channelFutureListenerOpt = CloseOpt)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause match {
      case e: java.io.IOException =>
        ctx.channel().close().addListener(CloseOpt.get)
      case _ =>
        cause.printStackTrace()
        logger.error(s"exception on query.", cause)
        simpleResponse(ctx, BadRequest, byteBufOpt = None, channelFutureListenerOpt = CloseOpt)
    }
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
  val transport = Try(config.getString("netty.transport")).recover { case _ => "jdk" }.get
  val maxBodySize = Try(config.getInt("max.body.size")).recover { case _ => 65536 * 2 }.get

  // init s2graph with config
  val s2graph = new S2Graph(config)(ec)
  val rest = new RestHandler(s2graph)(ec)

  val deployInfo = Try(Source.fromFile("./release_info").mkString("")).recover { case _ => "release info not found\n" }.get
  var isHealthy = config.getBooleanWithFallback("app.health.on", true)
  var isFallbackHealthy = true
  var isQueryFallbackHealthy = true

  logger.info(s"starts with num of thread: $numOfThread, ${threadPool.getClass.getSimpleName}")
  logger.info(s"transport: $transport")

  // Configure the server.
  val (bossGroup, workerGroup, channelClass) = transport match {
    case "native" =>
      (new EpollEventLoopGroup(1), new EpollEventLoopGroup(), classOf[EpollServerSocketChannel])
    case _ =>
      (new NioEventLoopGroup(1), new NioEventLoopGroup(), classOf[NioServerSocketChannel])
  }

  try {
    val b: ServerBootstrap = new ServerBootstrap()
      .option(ChannelOption.SO_BACKLOG, Int.box(2048))

    b.group(bossGroup, workerGroup).channel(channelClass)
      .handler(new LoggingHandler(LogLevel.INFO))
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel) {
          val p = ch.pipeline()
          p.addLast(new HttpServerCodec())
          p.addLast(new HttpObjectAggregator(maxBodySize))
          p.addLast(new S2RestHandler(rest)(ec))
        }
      })

    // for check server is started
    logger.info(s"Listening for HTTP on /0.0.0.0:$port")
    val ch: Channel = b.bind(port).sync().channel()
    ch.closeFuture().sync()

  } finally {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    s2graph.shutdown()
  }
}

