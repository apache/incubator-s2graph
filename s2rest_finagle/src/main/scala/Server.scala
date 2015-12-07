package com.kakao.s2graph.rest.netty

import com.kakao.s2graph.core._
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
import play.api.libs.json.Json

import scala.util.{Failure, Success}

class S2RestHandler extends SimpleChannelInboundHandler[FullHttpRequest] {

  import scala.concurrent.ExecutionContext.Implicits.global

  val CONTENT_TYPE = "Content-Type"
  val CONTENT_LENGTH = "Content-Length"
  val CONNECTION = "Connection"
  val KEEP_ALIVE = "keep-alive"

  override def channelRead0(ctx: ChannelHandlerContext, req: FullHttpRequest): Unit = {
    val jsonString = req.content.toString(CharsetUtil.UTF_8)
    val q = NettyServer.s2parser.toQuery(Json.parse(jsonString))
    val future = NettyServer.s2graph.getEdges(q)

    future onComplete {
      case Success(s2Res) =>
        val resJson = PostProcess.toSimpleVertexArrJson(s2Res)
        val buf: ByteBuf = Unpooled.copiedBuffer(resJson.toString, CharsetUtil.UTF_8)
        val res: FullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buf)

        res.headers().set(CONTENT_TYPE, "application/json; charset=utf-8")

        if (HttpHeaders.isKeepAlive(req)) {
          res.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
        }

        res.headers().set(CONTENT_LENGTH, buf.readableBytes())
        ctx.writeAndFlush(res).addListener(new ChannelFutureListener {
          override def operationComplete(future: ChannelFuture): Unit = {
            if (!HttpHeaders.isKeepAlive(req)) future.channel().close()
          }
        })

      case Failure(ex) =>
        val res: FullHttpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR)
        ctx.writeAndFlush(res).addListener(ChannelFutureListener.CLOSE)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close()
  }

}

class S2RestInitializer extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel) {
    val p = ch.pipeline()
    p.addLast(new HttpServerCodec())
    p.addLast(new HttpObjectAggregator(65536))
    p.addLast(new S2RestHandler())
  }
}

object NettyServer extends App {
  val config = ConfigFactory.load()

  // init s2graph with config
  val s2graph = new Graph(config)(scala.concurrent.ExecutionContext.Implicits.global)
  val s2parser = new RequestParser(s2graph)

  // app status code
  var isHealthy = true


  val Ssl = false
  val Port = try {
    config.getInt("http.port")
  } catch {
    case e: Exception => 9000
  }

  // Configure the server.
  val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
  val workerGroup: EventLoopGroup = new NioEventLoopGroup()

  try {
    val b: ServerBootstrap = new ServerBootstrap()
    b.option(ChannelOption.SO_BACKLOG, Int.box(2048))

    b.group(bossGroup, workerGroup).channel(classOf[NioServerSocketChannel])
      .handler(new LoggingHandler(LogLevel.INFO))
      .childHandler(new S2RestInitializer())

    val ch: Channel = b.bind(Port).sync().channel()
    ch.closeFuture().sync()

  } finally {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }
}

