package com.kakao.s2graph.core.parsers

import com.kakao.s2graph.core.GraphExceptions.WhereParserException
import com.kakao.s2graph.core._
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.types.InnerValLike

import scala.annotation.tailrec
import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

trait ExtractValue extends JSONParser {
  val parent = "_parent."

  def propToInnerVal(edge: Edge, key: String) = {
    val (propKey, parentEdge) = findParentEdge(edge, key)

    val label = parentEdge.label
    val metaPropInvMap = label.metaPropsInvMap
    val labelMeta = metaPropInvMap.getOrElse(propKey, throw WhereParserException(s"Where clause contains not existing property name: $propKey"))
    val metaSeq = labelMeta.seq

    metaSeq match {
      case LabelMeta.from.seq => parentEdge.srcVertex.innerId
      case LabelMeta.to.seq => parentEdge.tgtVertex.innerId
      case _ => parentEdge.propsWithTs.get(metaSeq) match {
        case None => toInnerVal(labelMeta.defaultValue, labelMeta.dataType, label.schemaVersion)
        case Some(edgeVal) => edgeVal.innerVal
      }
    }
  }

  def valueToCompare(edge: Edge, key: String, value: String) = {
    val label = edge.label
    if (value.startsWith(parent) || label.metaPropsInvMap.contains(value)) propToInnerVal(edge, value)
    else {
      val (propKey, _) = findParentEdge(edge, key)

      val labelMeta = label.metaPropsInvMap.getOrElse(propKey, throw WhereParserException(s"Where clause contains not existing property name: $propKey"))
      val (srcColumn, tgtColumn) = label.srcTgtColumn(edge.labelWithDir.dir)
      val dataType = propKey match {
        case "_to" | "to" => tgtColumn.columnType
        case "_from" | "from" => srcColumn.columnType
        case _ => labelMeta.dataType
      }
      toInnerVal(value, dataType, label.schemaVersion)
    }
  }

  @tailrec
  private def findParent(edge: Edge, depth: Int): Edge =
    if (depth > 0) findParent(edge.parentEdges.head.edge, depth - 1)
    else edge

  private def findParentEdge(edge: Edge, key: String): (String, Edge) = {
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

  def filter(edge: Edge): Boolean

  def binaryOp(binOp: (InnerValLike, InnerValLike) => Boolean)(propKey: String, value: String)(edge: Edge): Boolean = {
    val propValue = propToInnerVal(edge, propKey)
    val compValue = valueToCompare(edge, propKey, value)

    binOp(propValue, compValue)
  }
}

case class Where(clauses: Seq[Clause] = Seq.empty[Clause]) {
  def filter(edge: Edge) =
    if (clauses.isEmpty) true else clauses.map(_.filter(edge)).forall(identity)
}

case class Gt(propKey: String, value: String) extends Clause {
  override def filter(edge: Edge): Boolean = binaryOp(_ > _)(propKey, value)(edge)
}

case class Lt(propKey: String, value: String) extends Clause {
  override def filter(edge: Edge): Boolean = binaryOp(_ < _)(propKey, value)(edge)
}

case class Eq(propKey: String, value: String) extends Clause {
  override def filter(edge: Edge): Boolean = binaryOp(_ == _)(propKey, value)(edge)
}

case class InWithoutParent(label: Label, propKey: String, values: Set[String]) extends Clause {
  val innerValLikeLs = values.map { value =>
    val labelMeta = label.metaPropsInvMap.getOrElse(propKey, throw WhereParserException(s"Where clause contains not existing property name: $propKey"))
    val dataType = propKey match {
      case "_to" | "to" => label.tgtColumn.columnType
      case "_from" | "from" => label.srcColumn.columnType
      case _ => labelMeta.dataType
    }
    toInnerVal(value, dataType, label.schemaVersion)
  }
  override def filter(edge: Edge): Boolean = {
    val propVal = propToInnerVal(edge, propKey)
    innerValLikeLs.contains(propVal)
  }
}

case class IN(propKey: String, values: Set[String]) extends Clause {
  override def filter(edge: Edge): Boolean = {
    val propVal = propToInnerVal(edge, propKey)
    values.exists { value =>
      valueToCompare(edge, propKey, value) == propVal
    }
  }
}

case class Between(propKey: String, minValue: String, maxValue: String) extends Clause {
  override def filter(edge: Edge): Boolean = {
    val propVal = propToInnerVal(edge, propKey)
    val minVal = valueToCompare(edge, propKey, minValue)
    val maxVal = valueToCompare(edge, propKey, maxValue)

    minVal <= propVal && propVal <= maxVal
  }
}

case class Not(self: Clause) extends Clause {
  override def filter(edge: Edge) = !self.filter(edge)
}

case class And(left: Clause, right: Clause) extends Clause {
  override def filter(edge: Edge) = left.filter(edge) && right.filter(edge)
}

case class Or(left: Clause, right: Clause) extends Clause {
  override def filter(edge: Edge) = left.filter(edge) || right.filter(edge)
}

object WhereParser {
  val success = Where()
}

case class WhereParser(label: Label) extends JavaTokenParsers with JSONParser {

  val anyStr = "[^\\s(),]+".r

  val and = "and|AND".r

  val or = "or|OR".r

  val between = "between|BETWEEN".r

  val in = "in|IN".r

  val notIn = "not in|NOT IN".r

  def where: Parser[Where] = rep(clause) ^^ (Where(_))

  def paren: Parser[Clause] = "(" ~> clause <~ ")"

  def clause: Parser[Clause] = (predicate | paren) * (and ^^^ { (a: Clause, b: Clause) => And(a, b) } | or ^^^ { (a: Clause, b: Clause) => Or(a, b) })

  def identWithDot: Parser[String] = repsep(ident, ".") ^^ { case values => values.mkString(".") }

  def predicate = {
    identWithDot ~ ("!=" | "=") ~ anyStr ^^ {
      case f ~ op ~ s =>
        if (op == "=") Eq(f, s)
        else Not(Eq(f, s))
    } | identWithDot ~ (">=" | "<=" | ">" | "<") ~ anyStr ^^ {
      case f ~ op ~ s => op match {
        case ">" => Gt(f, s)
        case ">=" => Or(Gt(f, s), Eq(f, s))
        case "<" => Lt(f, s)
        case "<=" => Or(Lt(f, s), Eq(f, s))
      }
    } | identWithDot ~ (between ~> anyStr <~ and) ~ anyStr ^^ {
      case f ~ minV ~ maxV => Between(f, minV, maxV)
    } | identWithDot ~ (notIn | in) ~ ("(" ~> repsep(anyStr, ",") <~ ")") ^^ {
      case f ~ op ~ values =>
        val inClause =
          if (f.startsWith("_parent")) IN(f, values.toSet)
          else InWithoutParent(label, f, values.toSet)
        if (op.toLowerCase == "in") inClause
        else Not(inClause)


      case _ => throw WhereParserException(s"Failed to parse where clause. ")
    }
  }

  def parse(sql: String): Try[Where] = Try {
    parseAll(where, sql) match {
      case Success(r, q) => r
      case fail => throw WhereParserException(s"Where parsing error: ${fail.toString}")
    }
  }
}
