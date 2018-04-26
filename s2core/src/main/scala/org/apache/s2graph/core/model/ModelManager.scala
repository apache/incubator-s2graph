package org.apache.s2graph.core.model

import com.typesafe.config.Config
import org.apache.s2graph.core.schema.Label
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.core.{Fetcher, S2GraphLike}

import scala.concurrent.{ExecutionContext, Future}

object ModelManager {
  val FetcherClassNameKey = "fetchClassName"
  val ImporterClassNameKey = "importerClassName"
}

class ModelManager(s2GraphLike: S2GraphLike) {
  import ModelManager._

  private val fetcherPool = scala.collection.mutable.Map.empty[String, Fetcher]
  private val ImportLock = new java.util.concurrent.ConcurrentHashMap[String, Importer]

  def toImportLockKey(label: Label): String = label.label

  def getFetcher(label: Label): Fetcher = {
    fetcherPool.getOrElse(toImportLockKey(label), throw new IllegalStateException(s"$label is not imported."))
  }

  def initImporter(config: Config): Importer = {
    val className = config.getString(ImporterClassNameKey)

    Class.forName(className)
      .getConstructor(classOf[S2GraphLike])
      .newInstance(s2GraphLike)
      .asInstanceOf[Importer]
  }

  def initFetcher(config: Config)(implicit ec: ExecutionContext): Future[Fetcher] = {
    val className = config.getString(FetcherClassNameKey)

    val fetcher = Class.forName(className)
        .getConstructor(classOf[S2GraphLike])
      .newInstance(s2GraphLike)
      .asInstanceOf[Fetcher]

    fetcher.init(config)
  }

  def importModel(label: Label, config: Config)(implicit ec: ExecutionContext): Future[Importer] = {
    val importer = ImportLock.computeIfAbsent(toImportLockKey(label), new java.util.function.Function[String, Importer] {
      override def apply(k: String): Importer = {
        val importer = initImporter(config.getConfig("importer"))

        //TODO: Update Label's extra options.
        importer
          .run(config.getConfig("importer"))
          .map { importer =>
            logger.info(s"Close importer")
            importer.close()

            initFetcher(config.getConfig("fetcher")).map { fetcher =>
              importer.setStatus(true)


              fetcherPool
                .remove(k)
                .foreach { oldFetcher =>
                  logger.info(s"Delete old storage ($k) => $oldFetcher")
                  oldFetcher.close()
                }

              fetcherPool += (k -> fetcher)
              true
            }

            true
          }
          .onComplete { _ =>
            logger.info(s"ImportLock release: $k")
            ImportLock.remove(k)
          }
        importer
      }
    })

    Future.successful(importer)
  }

}
