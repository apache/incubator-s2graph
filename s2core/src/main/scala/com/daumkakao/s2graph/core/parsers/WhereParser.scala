package com.daumkakao.s2graph.core.parsers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.mysqls._
import com.daumkakao.s2graph.core.types.InnerValLike

import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers


trait Clause extends JSONParser {
  def and(otherField: Clause): Clause = And(this, otherField)

  def or(otherField: Clause): Clause = Or(this, otherField)

  def filter(edge: Edge): Boolean

  def propToInnerVal(edge: Edge, propKey: String) = makeCompareTarget(edge, propKey, None)._1

  def compToInenrVal(edge: Edge, propKey: String, value: String) = makeCompareTarget(edge, propKey, Some(value))._2

  // TODO: Get label meta info from cache
  private def makeCompareTarget(edgeToCompare: Edge, key: String, valueToCompare: Option[String]): (InnerValLike, InnerValLike) = {
    def findCompareEdge(edge: Edge, depth: Int): Edge =
      if (depth > 0) findCompareEdge(edge.parentEdges.head.edge, depth - 1)
      else edge

    val splatted = key.split("parent.")
    val depth = splatted.length - 1
    val propKey = LabelMeta.fixMetaName(splatted.last)

    val edge = findCompareEdge(edgeToCompare, depth)
    val label = edge.label
    val schemaVersion = label.schemaVersion
    val metaPropInvMap = label.metaPropsInvMap
    val labelMeta = metaPropInvMap.getOrElse(propKey, throw new RuntimeException(s"Where clause contains not existing property name: $propKey"))
    val metaSeq = labelMeta.seq

    val defaultInnerVal = toInnerVal(labelMeta.defaultValue, labelMeta.dataType, label.schemaVersion)
    val innerVal = valueToCompare match {
      case Some(value) => toInnerVal(value, labelMeta.dataType, schemaVersion)
      case None => defaultInnerVal
    }

    metaSeq match {
      case LabelMeta.from.seq => edge.srcVertex.innerId -> innerVal
      case LabelMeta.to.seq => edge.tgtVertex.innerId -> innerVal
      case _ => edge.propsWithTs.get(metaSeq) match {
        case None => defaultInnerVal -> innerVal
        case Some(edgeVal) => edgeVal.innerVal -> innerVal
      }
    }
  }

  def binaryOp(binOp: (InnerValLike, InnerValLike) => Boolean)(propKey: String, value: String)(edge: Edge): Boolean = {
    val propValue = propToInnerVal(edge, propKey)
    val compValue = compToInenrVal(edge, propKey, value)
    binOp(propValue, compValue)
  }
}

case class Where(clauses: Seq[Clause] = Seq.empty[Clause]) {
  def filter(edge: Edge) = clauses.map(_.filter(edge)).forall(identity)
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

case class IN(propKey: String, values: Set[String]) extends Clause {
  override def filter(edge: Edge): Boolean = {
    val propVal = propToInnerVal(edge, propKey)
    val compValues = values.map { value => compToInenrVal(edge, propKey, value) }

    compValues.contains(propVal)
  }
}

case class Between(propKey: String, minValue: String, maxValue: String) extends Clause {
  override def filter(edge: Edge): Boolean = {
    val propVal = propToInnerVal(edge, propKey)
    val minCompVal = compToInenrVal(edge, propKey, minValue)
    val maxCompVal = compToInenrVal(edge, propKey, maxValue)

    minCompVal <= propVal && propVal <= maxCompVal
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

case class WhereParser(labelMap: Map[String, Label]) extends JavaTokenParsers with JSONParser {

  //  val metaProps = label.metaPropsInvMap

  val anyStr = "[^\\s(),]+".r

  val and = "and|AND".r

  val or = "or|OR".r

  val between = "between|BETWEEN".r

  val in = "in|IN".r

  val notIn = "not in|NOT IN".r

  def where: Parser[Where] = rep(clause) ^^ (Where(_))

  def paren: Parser[Clause] = "(" ~> clause <~ ")"

  def clause: Parser[Clause] = (predicate | paren) * (and ^^^ { (a: Clause, b: Clause) => And(a, b) } | or ^^^ { (a: Clause, b: Clause) => Or(a, b) })

  def predicate = {
    ident ~ ("!=" | "=") ~ anyStr ^^ {
      case f ~ op ~ s =>
        if (op == "=") Eq(f, s)
        else Not(Eq(f, s))
    } | ident ~ (">=" | "<=" | ">" | "<") ~ anyStr ^^ {
      case f ~ op ~ s => op match {
        case ">" => Gt(f, s)
        case ">=" => Or(Gt(f, s), Eq(f, s))
        case "<" => Lt(f, s)
        case "<=" => Or(Lt(f, s), Eq(f, s))
      }
    }
  } | ident ~ (between ~> anyStr <~ and) ~ anyStr ^^ {
    case f ~ minV ~ maxV =>
      Between(f, minV, maxV)
  } | ident ~ (notIn | in) ~ ("(" ~> repsep(anyStr, ",") <~ ")") ^^ {
    case f ~ op ~ vals =>
      if (op.toLowerCase == "in") IN(f, vals.toSet)
      else Not(IN(f, vals.toSet))
    case _ => throw new RuntimeException(s"Failed to parse where clause. ")
  }

  def parse(sql: String): Try[Where] = {
    try {
      parseAll(where, sql) match {
        case Success(r, q) => scala.util.Success(r)
        case fail => scala.util.Failure(new RuntimeException(s"Where parsing error: ${
          fail.toString
        }"))
      }
    } catch {
      case ex: Exception => scala.util.Failure(ex)
    }
  }
}
