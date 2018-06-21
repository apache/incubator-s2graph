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

package org.apache.s2graph.core.parsers

import org.apache.s2graph.core.GraphExceptions.WhereParserException
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core._
import org.apache.s2graph.core.types.InnerValLike

import scala.annotation.tailrec
import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

trait ExtractValue {
  val parent = "_parent."

  private def throwEx(key: String) = {
    throw WhereParserException(s"Where clause contains not existing property name: $key")
  }

  def propToInnerVal(element: GraphElement, key: String): InnerValLike = {
    element match {
      case e: S2EdgeLike => edgePropToInnerVal(e, key)
      case v: S2VertexLike => vertexPropToInnerVal(v, key)
      case _ => throw new IllegalArgumentException("only S2EdgeLike/S2VertexLike supported.")
    }
  }

  def valueToCompare(element: GraphElement, key: String, value: String): InnerValLike = {
    element match {
      case e: S2EdgeLike => edgeValueToCompare(e, key, value)
      case v: S2VertexLike => vertexValueToCompare(v, key, value)
      case _ => throw new IllegalArgumentException("only S2EdgeLike/S2VertexLike supported.")
    }
  }

  private def edgePropToInnerVal(edge: S2EdgeLike, key: String): InnerValLike = {
    val (propKey, parentEdge) = findParentEdge(edge, key)

    val innerValLikeWithTs =
      S2EdgePropertyHelper.propertyValue(parentEdge, propKey).getOrElse(throwEx(propKey))

    innerValLikeWithTs.innerVal
  }

  private def vertexPropToInnerVal(vertex: S2VertexLike, key: String): InnerValLike = {
    S2VertexPropertyHelper.propertyValue(vertex, key).getOrElse(throwEx(key))
  }

  private def edgeValueToCompare(edge: S2EdgeLike, key: String, value: String): InnerValLike = {
    val label = edge.innerLabel
    if (value.startsWith(parent) || label.metaPropsInvMap.contains(value)) propToInnerVal(edge, value)
    else {
      val (propKey, _) = findParentEdge(edge, key)

      val labelMeta = label.metaPropsInvMap.getOrElse(propKey, throw WhereParserException(s"Where clause contains not existing property name: $propKey"))
      val (srcColumn, tgtColumn) = label.srcTgtColumn(edge.getDir())
      val dataType = propKey match {
        case "_to" | "to" => tgtColumn.columnType
        case "_from" | "from" => srcColumn.columnType
        case _ => labelMeta.dataType
      }
      toInnerVal(value, dataType, label.schemaVersion)
    }
  }

  private def vertexValueToCompare(vertex: S2VertexLike, key: String, value: String): InnerValLike = {
    val columnMeta = vertex.serviceColumn.metasInvMap.getOrElse(key, throw WhereParserException(s"Where clause contains not existing property name: $key"))

    toInnerVal(value, columnMeta.dataType, vertex.serviceColumn.schemaVersion)
  }

  @tailrec
  private def findParent(edge: S2EdgeLike, depth: Int): S2EdgeLike =
    if (depth > 0) findParent(edge.getParentEdges().head.edge, depth - 1)
    else edge

  private def findParentEdge(edge: S2EdgeLike, key: String): (String, S2EdgeLike) = {
    if (!key.startsWith(parent)) (key, edge)
    else {
      val split = key.split(parent)
      val depth = split.length - 1
      val propKey = split.last

      val parentEdge = findParent(edge, depth)

      (propKey, parentEdge)
    }
  }
}

trait Clause extends ExtractValue {
  def and(otherField: Clause): Clause = And(this, otherField)

  def or(otherField: Clause): Clause = Or(this, otherField)

  def filter(element: GraphElement): Boolean

  def binaryOp(binOp: (InnerValLike, InnerValLike) => Boolean)(propKey: String, value: String)(element: GraphElement): Boolean = {
    val propValue = propToInnerVal(element, propKey)
    val compValue = valueToCompare(element, propKey, value)

    binOp(propValue, compValue)
  }
}

object Where {
  def apply(sql: String): Try[Where] = {
    val parser = new WhereParser()

    parser.parse(sql)
  }
}

case class Where(clauses: Seq[Clause] = Seq.empty[Clause]) {
  def filter(element: GraphElement) =
    if (clauses.isEmpty) true else clauses.map(_.filter(element)).forall(identity)
}

case class Gt(propKey: String, value: String) extends Clause {
  override def filter(element: GraphElement): Boolean = binaryOp(_ > _)(propKey, value)(element)
}

case class Lt(propKey: String, value: String) extends Clause {
  override def filter(element: GraphElement): Boolean = binaryOp(_ < _)(propKey, value)(element)
}

case class Eq(propKey: String, value: String) extends Clause {
  override def filter(element: GraphElement): Boolean = binaryOp(_ == _)(propKey, value)(element)
}

case class InWithoutParent(propKey: String, values: Set[String]) extends Clause {
  override def filter(element: GraphElement): Boolean = {
    val propVal = propToInnerVal(element, propKey)
    val innerVaLs = values.map { value =>
      valueToCompare(element, propKey, value)
    }

    innerVaLs(propVal)
  }
}

case class Contains(propKey: String, value: String) extends Clause {
  override def filter(element: GraphElement): Boolean = {
    val propVal = propToInnerVal(element, propKey)
    propVal.value.toString.contains(value)
  }
}

case class IN(propKey: String, values: Set[String]) extends Clause {
  override def filter(element: GraphElement): Boolean = {
    val propVal = propToInnerVal(element, propKey)
    values.exists { value =>
      valueToCompare(element, propKey, value) == propVal
    }
  }
}

case class Between(propKey: String, minValue: String, maxValue: String) extends Clause {
  override def filter(element: GraphElement): Boolean = {
    val propVal = propToInnerVal(element, propKey)
    val minVal = valueToCompare(element, propKey, minValue)
    val maxVal = valueToCompare(element, propKey, maxValue)

    minVal <= propVal && propVal <= maxVal
  }
}

case class Not(self: Clause) extends Clause {
  override def filter(element: GraphElement) = !self.filter(element)
}

case class And(left: Clause, right: Clause) extends Clause {
  override def filter(element: GraphElement) = left.filter(element) && right.filter(element)
}

case class Or(left: Clause, right: Clause) extends Clause {
  override def filter(element: GraphElement) = left.filter(element) || right.filter(element)
}

object WhereParser {
  val success = Where()
}

case class WhereParser() extends JavaTokenParsers {
  override val stringLiteral = (("'" ~> "(\\\\'|[^'])*".r <~ "'") ^^ (_.replace("\\'", "'"))) | anyStr

  val anyStr = "[^\\s(),']+".r

  val and = "and|AND".r

  val or = "or|OR".r

  val between = "between|BETWEEN".r

  val in = "in|IN".r

  val notIn = "not in|NOT IN".r

  val contains = "contains|CONTAINS".r
  
  def where: Parser[Where] = rep(clause) ^^ (Where(_))

  def paren: Parser[Clause] = "(" ~> clause <~ ")"

  def clause: Parser[Clause] = (_not | predicate | paren) * (and ^^^ { (a: Clause, b: Clause) => And(a, b) } | or ^^^ { (a: Clause, b: Clause) => Or(a, b) })

  def identWithDot: Parser[String] = repsep(ident, ".") ^^ { case values => values.mkString(".") }

  val _not = "not|NOT".r ~ (predicate | paren) ^^ {
    case op ~ p => Not(p)
  }

  val _eq = identWithDot ~ ("!=" | "=") ~ stringLiteral ^^ {
    case f ~ op ~ s => if (op == "=") Eq(f, s) else Not(Eq(f, s))
  }

  val _ltGt = identWithDot ~ (">=" | "<=" | ">" | "<") ~ stringLiteral ^^ {
    case f ~ op ~ s => op match {
      case ">" => Gt(f, s)
      case ">=" => Or(Gt(f, s), Eq(f, s))
      case "<" => Lt(f, s)
      case "<=" => Or(Lt(f, s), Eq(f, s))
    }
  }

  val _between = identWithDot ~ (between ~> stringLiteral <~ and) ~ stringLiteral ^^ {
    case f ~ minV ~ maxV => Between(f, minV, maxV)
  }

  val _in = identWithDot ~ (notIn | in) ~ ("(" ~> repsep(stringLiteral, ",") <~ ")") ^^ {
    case f ~ op ~ values =>
      val inClause =
        if (f.startsWith("_parent")) IN(f, values.toSet)
        else InWithoutParent(f, values.toSet)

      if (op.toLowerCase == "in") inClause
      else Not(inClause)
  }

  val _contains = identWithDot ~ contains ~ stringLiteral ^^ {
    case f ~ op ~ value => Contains(f, value)
  }

  def predicate = _eq | _ltGt | _between | _in | _contains

  def parse(sql: String): Try[Where] = Try {
    parseAll(where, sql) match {
      case Success(r, q) => r
      case fail => throw WhereParserException(s"Where parsing error: ${fail.toString}")
    }
  }
}
