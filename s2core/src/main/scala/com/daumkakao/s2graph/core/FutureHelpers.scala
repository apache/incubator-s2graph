//package com.daumkakao.s2graph.core
//
//import scala.concurrent.{ ExecutionContext, CanAwait, Awaitable, Future, Promise }
//import scala.concurrent.duration.Duration
//import scala.util.Try
//import org.jboss.netty.util.{ TimerTask, HashedWheelTimer }
//import java.util.concurrent.{ TimeoutException, TimeUnit }
//import org.jboss.netty.util.Timeout
//import play.api.Play.current
//import java.util.concurrent.atomic.{ AtomicReference, AtomicBoolean }
//import java.util.concurrent.CancellationException
//import scala.concurrent.duration._
//import play.libs.Akka
//
//object TimeoutFuture {
//  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
//  implicit class KestrelCombinator[A](val a: A) extends AnyVal {
//    def withSideEffect(fun: A => Unit): A = { fun(a); a }
//    def tap(fun: A => Unit): A = withSideEffect(fun)
//  }
//
//  def apply[T](op: => Future[T], fallback: => T)(implicit ec: ExecutionContext, after: Duration): Future[T] = {
//    // Creating the timer inside of the apply is for isolated/limited limited use of TimeoutFuture only,
//    // to keep from running a HashWheelTimer (or managing its running manually).
//    // You likely shouldn't use this a ton (there's better patterns if this is common for you).
//    // However, if it's for more than just isolated use, move this line outside of the apply:
//    val promise = Promise[T]()
//    val timeout = timer.newTimeout(new TimerTask {
//      def run(timeout: Timeout) {
//        Logger.error(s"Future timed out after ${after.toMillis}ms")
//        promise.success(fallback)
//        //        promise.failure(new TimeoutException(s"Future timed out after ${after.toMillis}ms"))
//      }
//    }, after.toNanos, TimeUnit.NANOSECONDS)
//    // does not cancel future, only resolves result in approx duration. Your future may still be running!
//    Future.firstCompletedOf(Seq(op, promise.future)).tap(_.onComplete { case result => timeout.cancel() })
//  }
//}
//object CancellableFuture {
//  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
//
//  def apply[T](fun: => T, after: Duration)(onTimeout: => T)(implicit ec: ExecutionContext): Future[T] = {
//    val startedAt = System.currentTimeMillis()
//    val promise = Promise[T]
//    val timeoutPromise = Promise[T]
//    val threadRef = new AtomicReference[Thread](null)
//    promise tryCompleteWith Future {
//      val t = Thread.currentThread()
//      threadRef.synchronized { threadRef.set(t) }
//      try fun finally { threadRef.synchronized(threadRef.set(null)) }
//    }
//
//    val timeout = timer.newTimeout(new TimerTask {
//      def run(timeout: Timeout) {
//        val prevRef = threadRef.getAndSet(null)
//        val ts = System.currentTimeMillis()
//        Logger.info(s"interrupt ${prevRef}: after[${ts - startedAt}]")
//        prevRef.interrupt()
//        //        timeoutPromise.failure(new TimeoutException(s"Future timed out after ${after.toMillis}ms"))
//        timeoutPromise.success(onTimeout)
//      }
//    }, after.toNanos, TimeUnit.NANOSECONDS)
//
//    val f = Future.firstCompletedOf(Seq(promise.future, timeoutPromise.future))
//    f.onComplete { case _ => timeout.cancel() }
//    f
//  }
//}