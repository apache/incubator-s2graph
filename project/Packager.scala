import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor, StandardCopyOption}

import sbt.Keys._
import sbt._

object Packager {

  val packagedJars = TaskKey[Seq[File]]("packaged-jars", "list of jar files for packaging")
  val packager = TaskKey[File]("package", s"packages the project for deployment")

  val packagedJarsTask = (state, thisProjectRef).flatMap { (state, project) =>

    /** return all tasks of given TaskKey's dependencies */
    def taskInAllDependencies[T](taskKey: TaskKey[T]): Task[Seq[T]] = {
      val structure = Project.structure(state)

      Dag.topologicalSort(project) { ref =>
        Project.getProject(ref, structure).toList.flatMap { p =>
          p.dependencies.map(_.project) ++ p.aggregate
        }
      }.flatMap { p =>
        taskKey in p get structure.data
      }.join
    }

    for {
      packaged <- taskInAllDependencies(packagedArtifacts)
      srcs <- taskInAllDependencies(packageSrc in Compile)
      docs <- taskInAllDependencies(packageDoc in Compile)
    } yield {
      packaged.flatten
        .collect { case (artifact, path) if artifact.extension == "jar" => path }
        .diff(srcs ++ docs) //remove srcs & docs since we do not need them in the dist
        .distinct
    }
  }

  val packagerTask = (baseDirectory, packagedJars, dependencyClasspath in Runtime, target, streams, version).map {
    (root, packaged, dependencies, target, streams, version) =>
      val name = s"s2graph-$version-incubating-bin"
      val base = target / name

      IO.delete(base)
      IO.createDirectory(base)

      val subdirectories = Seq("bin", "conf", "lib")
      val files = Seq("LICENSE", "NOTICE", "DISCLAIMER")

      for (dir <- subdirectories) {
        IO.createDirectory(base / dir)
      }

      for (file <- files) {
        val source = (root / file).toPath
        val target = (base / file).toPath
        Files.copy(source, target)
      }

      // copy jars to /lib
      val libs = dependencies.filter(_.data.ext == "jar").map(_.data) ++ packaged
      libs.foreach { jar =>
        val source = jar.toPath
        val target = Paths.get(base.getPath, "lib", jar.getName)

        try {
          // try to create hard links for speed and to save disk space
          Files.createLink(target, source)
          streams.log.debug(s"Created link: $source -> $target")
        } catch {
          case e: Any =>
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
            streams.log.debug(s"Copied: $source -> $target")
        }
      }

      // copy other files recursively
      for (subdir <- subdirectories if subdir != "lib") {
        val source = (root / subdir).toPath
        val target = (base / subdir).toPath
        Files.walkFileTree(source, new SimpleFileVisitor[Path] {
          override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
            val dest = target.resolve(source.relativize(dir))
            Files.createDirectories(dest)
            streams.log.debug(s"Created directory: $dest")
            FileVisitResult.CONTINUE
          }

          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            val dest = target.resolve(source.relativize(file))
            Files.copy(file, dest)
            streams.log.debug(s"Copied: $file -> $dest")
            FileVisitResult.CONTINUE
          }
        })
      }

      streams.log.info(s"Package for distribution located at $base")

      {
        import scala.sys.process._
        streams.log.info(s"creating a tarball...")
        s"tar zcf $base.tar.gz $base".!!
        streams.log.info(s"Tarball is located at $base.tar.gz")
      }

      base
  }

  val defaultSettings = Seq(
    packagedJars <<= packagedJarsTask,
    packager <<= packagerTask
  )
}
