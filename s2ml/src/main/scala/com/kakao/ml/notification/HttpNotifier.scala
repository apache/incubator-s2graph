package com.kakao.ml.notification

import java.io.{BufferedReader, InputStreamReader}
import java.util

import com.kakao.ml.{BaseDataProcessor, Params, PredecessorData}
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicNameValuePair
import org.apache.spark.sql.SQLContext

case class HttpNotifierItem(url: String, params: Map[String, String])

case class HttpNotifierParams(urls: Seq[HttpNotifierItem]) extends Params

class HttpNotifier(params: HttpNotifierParams) extends BaseDataProcessor[PredecessorData, PredecessorData](params) {

  override protected def processBlock(sQLContext: SQLContext, input: PredecessorData): PredecessorData = {
    val info = predecessorData.asMap.map(x => s"${x._1}:${x._2.getClass.getName}").mkString("\n")

    val client = HttpClientBuilder.create().build()

    params.urls.foreach { case HttpNotifierItem(url, p) =>
      val post = new HttpPost(url)

      val urlParameters = new util.ArrayList[NameValuePair]()
      p.foreach { case (key, value) =>
        urlParameters.add(new BasicNameValuePair(key, value.replace("{}", info)))
      }

      post.setEntity(new UrlEncodedFormEntity(urlParameters))

      val response = client.execute(post)

      val reader = new BufferedReader(new InputStreamReader(response.getEntity.getContent))

      while (reader.readLine() != null) {}
    }

    client.close()

    predecessorData
  }
}
