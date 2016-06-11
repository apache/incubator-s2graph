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

  val packagerTask = (baseDirectory, packagedJars, dependencyClasspath in Runtime, target, streams).map {
    (root, packaged, dependencies, target, streams) =>
      val base = target / "deploy"

      IO.delete(base)
      IO.createDirectory(base)

      val subdirectories = Seq("bin", "conf", "lib")

      for (dir <- subdirectories) {
        IO.createDirectory(base / dir)
      }

      // copy jars to /lib
      val libs = dependencies.filter(_.data.ext == "jar").map(_.data) ++ packaged
      libs.foreach { jar =>
        val source = jar.toPath
        val target = Paths.get(base.getPath, "lib", jar.getName)

        try {
          // try to create hard links for speed and to save disk space
          Files.createLink(target, source)
          streams.log.debug(s"created link: $source -> $target")
        } catch {
          case e: Any =>
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING)
            streams.log.debug(s"copied: $source -> $target")
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
            streams.log.debug(s"created directory: $dest")
            FileVisitResult.CONTINUE
          }

          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            val dest = target.resolve(source.relativize(file))
            Files.copy(file, dest)
            streams.log.debug(s"copied: $file -> $dest")
            FileVisitResult.CONTINUE
          }
        })
      }

      streams.log.info(s"package for distribution located at $base")
      base
  }

  val defaultSettings = Seq(
    packagedJars <<= packagedJarsTask,
    packager <<= packagerTask
  )
}
