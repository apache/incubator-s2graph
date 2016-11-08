import sbt.Keys._
import sbt._
import scala.xml.XML

object Publisher {

  val defaultSettings = Seq(
    publish := {}, // disable unsigned publishing
    publishMavenStyle := true,
    publishTo := {
      if (isSnapshot.value) {
        Some("apache" at "https://repository.apache.org/content/repositories/snapshots")
      } else {
        Some("apache" at "https://repository.apache.org/content/repositories/releases")
      }
    },
    credentials ++= {
      val xml = XML.loadFile(new File(System.getProperty("user.home")) / ".m2" / "settings.xml")
      for (server <- xml \\ "server" if (server \ "id").text == "apache") yield {
        Credentials("Sonatype Nexus Repository Manager", "repository.apache.org", (server \ "username").text, (server \ "password").text)
      }
    },
    pomIncludeRepository := { _ => false },
    pomExtra := {
      <url>https://github.com/apache/incubator-s2graph</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        </license>
      </licenses>
      <scm>
        <connection>scm:git://git.apache.org/incubator-s2graph.git</connection>
        <developerConnection>scm:git:https://git-wip-us.apache.org/repos/asf/incubator-s2graph.git</developerConnection>
        <url>github.com/apache/incubator-s2graph</url>
      </scm>
      <developers>
        <developer>
          <id>s2graph</id>
          <name>S2Graph Team</name>
          <url>http://s2graph.incubator.apache.org/</url>
        </developer>
      </developers>
      <mailingLists>
        <mailingList>
          <name>Dev Mailing List</name>
          <post>dev@s2graph.incubator.apache.org</post>
          <subscribe>dev-subscribe@s2graph.incubator.apache.org</subscribe>
          <unsubscribe>dev-unsubscribe@s2graph.incubator.apache.org</unsubscribe>
        </mailingList>
        <mailingList>
          <name>User Mailing List</name>
          <post>users@s2graph.incubator.apache.org</post>
          <subscribe>users-subscribe@s2graph.incubator.apache.org</subscribe>
          <unsubscribe>users-unsubscribe@s2graph.incubator.apache.org</unsubscribe>
        </mailingList>
        <mailingList>
          <name>Commits Mailing List</name>
          <post>commits@s2graph.incubator.apache.org</post>
          <subscribe>commits-subscribe@s2graph.incubator.apache.org</subscribe>
          <unsubscribe>commits-unsubscribe@s2graph.incubator.apache.org</unsubscribe>
        </mailingList>
      </mailingLists>
    }
  )
}
