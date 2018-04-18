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

import org.apache.s2graph.core.GraphExceptions.{LabelNotExistException, WhereParserException}
import org.apache.s2graph.core.mysqls.{Label, LabelMeta}
import org.apache.s2graph.core.types.InnerValLike
import org.apache.s2graph.core.{S2EdgeLike}
import org.apache.s2graph.core.JSONParser._

import scala.annotation.tailrec
import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

trait ExtractValue {
  val parent = "_parent."

  def propToInnerVal(edge: S2EdgeLike, key: String) = {
    val (propKey, parentEdge) = findParentEdge(edge, key)

    val label = parentEdge.innerLabel
    val metaPropInvMap = label.metaPropsInvMap
    val labelMeta = metaPropInvMap.getOrElse(propKey, throw WhereParserException(s"Where clause contains not existing property name: $propKey"))

    labelMeta match {
      case LabelMeta.from => parentEdge.srcVertex.innerId
      case LabelMeta.to => parentEdge.tgtVertex.innerId
      case _ => parentEdge.propertyValueInner(labelMeta).innerVal
    }
  }

  def valueToCompare(edge: S2EdgeLike, key: String, value: String) = {
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

  def filter(edge: S2EdgeLike): Boolean

  def binaryOp(binOp: (InnerValLike, InnerValLike) => Boolean)(propKey: String, value: String)(edge: S2EdgeLike): Boolean = {
    val propValue = propToInnerVal(edge, propKey)
    val compValue = valueToCompare(edge, propKey, value)

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
  def filter(edge: S2EdgeLike) =
    if (clauses.isEmpty) true else clauses.map(_.filter(edge)).forall(identity)
}

case class Gt(propKey: String, value: String) extends Clause {
  override def filter(edge: S2EdgeLike): Boolean = binaryOp(_ > _)(propKey, value)(edge)
}

case class Lt(propKey: String, value: String) extends Clause {
  override def filter(edge: S2EdgeLike): Boolean = binaryOp(_ < _)(propKey, value)(edge)
}

case class Eq(propKey: String, value: String) extends Clause {
  override def filter(edge: S2EdgeLike): Boolean = binaryOp(_ == _)(propKey, value)(edge)
}

case class InWithoutParent(propKey: String, values: Set[String]) extends Clause {
  override def filter(edge: S2EdgeLike): Boolean = {
    val label = edge.innerLabel

    val propVal = propToInnerVal(edge, propKey)
    val innerVaLs = values.map { value =>
      toInnerVal(value, label.metaPropsInvMap(propKey).dataType, label.schemaVersion)
    }

    innerVaLs(propVal)
  }
}

case class Contains(propKey: String, value: String) extends Clause {
  override def filter(edge: S2EdgeLike): Boolean = {
    val propVal = propToInnerVal(edge, propKey)
    propVal.value.toString.contains(value)
  }
}

case class IN(propKey: String, values: Set[String]) extends Clause {
  override def filter(edge: S2EdgeLike): Boolean = {
    val propVal = propToInnerVal(edge, propKey)
    values.exists { value =>
      valueToCompare(edge, propKey, value) == propVal
    }
  }
}

case class Between(propKey: String, minValue: String, maxValue: String) extends Clause {
  override def filter(edge: S2EdgeLike): Boolean = {
    val propVal = propToInnerVal(edge, propKey)
    val minVal = valueToCompare(edge, propKey, minValue)
    val maxVal = valueToCompare(edge, propKey, maxValue)

    minVal <= propVal && propVal <= maxVal
  }
}

case class Not(self: Clause) extends Clause {
  override def filter(edge: S2EdgeLike) = !self.filter(edge)
}

case class And(left: Clause, right: Clause) extends Clause {
  override def filter(edge: S2EdgeLike) = left.filter(edge) && right.filter(edge)
}

case class Or(left: Clause, right: Clause) extends Clause {
  override def filter(edge: S2EdgeLike) = left.filter(edge) || right.filter(edge)
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

  val contains = "contains|CONTAINS".r

  val notIn = "not in|NOT IN".r

  def where: Parser[Where] = rep(clause) ^^ (Where(_))

  def paren: Parser[Clause] = "(" ~> clause <~ ")"

  def clause: Parser[Clause] = (predicate | paren) * (and ^^^ { (a: Clause, b: Clause) => And(a, b) } | or ^^^ { (a: Clause, b: Clause) => Or(a, b) })

  def identWithDot: Parser[String] = repsep(ident, ".") ^^ { case values => values.mkString(".") }

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
