package com.daumkakao.s2graph.core.parsers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.mysqls._
import com.daumkakao.s2graph.core.types2.InnerValLike

import scala.util.Try
import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Created by shon on 5/30/15.
 */

trait Clause extends JSONParser {
  def and(otherField: Clause): Clause = And(this, otherField)

  def or(otherField: Clause): Clause = Or(this, otherField)

  def filter(edge: Edge): Boolean
}

case class Where(clauses: Seq[Clause] = Seq.empty[Clause]) {
  def filter(edge: Edge) = clauses.map(_.filter(edge)).forall(identity)
}

case class Equal(propKey: Byte, value: InnerValLike) extends Clause {
  override def filter(edge: Edge): Boolean = {
    propKey match {
      case LabelMeta.from.seq => edge.srcVertex.innerId == value
      case LabelMeta.to.seq => edge.tgtVertex.innerId == value
      case _ =>
        edge.propsWithTs.get(propKey) match {
          case None =>
            val label = edge.label
            val meta = label.metaPropsMap(propKey)
            val defaultValue = toInnerVal(meta.defaultValue, meta.dataType, label.schemaVersion)
            defaultValue == value
          case Some(edgeVal) => edgeVal.innerVal == value
        }
    }
  }
}

case class IN(propKey: Byte, values: Set[InnerValLike]) extends Clause {
  override def filter(edge: Edge): Boolean = {
    propKey match {
      case LabelMeta.from.seq => values.contains(edge.srcVertex.innerId)
      case LabelMeta.to.seq => values.contains(edge.tgtVertex.innerId)
      case _ =>
        edge.propsWithTs.get(propKey) match {
          case None =>
            val label = edge.label
            val meta = label.metaPropsMap(propKey)
            val defaultValue = toInnerVal(meta.defaultValue, meta.dataType, label.schemaVersion)
            values.contains(defaultValue)
          case Some(edgeVal) => values.contains(edgeVal.innerVal)
        }
    }
  }
}

case class Between(propKey: Byte, minValue: InnerValLike, maxValue: InnerValLike) extends Clause {
  override def filter(edge: Edge): Boolean = {
    propKey match {
      case LabelMeta.from.seq => minValue <= edge.srcVertex.innerId && edge.srcVertex.innerId <= maxValue
      case LabelMeta.to.seq => minValue <= edge.tgtVertex.innerId && edge.tgtVertex.innerId <= maxValue
      case _ =>
        edge.propsWithTs.get(propKey) match {
          case None =>
            val label = edge.label
            val meta = label.metaPropsMap(propKey)
            val defaultValue = toInnerVal(meta.defaultValue, meta.dataType, label.schemaVersion)
            minValue <= defaultValue && defaultValue <= maxValue
          case Some(edgeVal) =>
            minValue <= edgeVal.innerVal && edgeVal.innerVal <= maxValue
        }
    }
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

case class WhereParser(label: Label) extends JavaTokenParsers with JSONParser {

  val metaProps = label.metaPropsInvMap

  val anyStr = "[^\\s(),]+".r

  def where: Parser[Where] = rep(clause) ^^ (Where(_))

  def paren: Parser[Clause] = "(" ~> clause <~ ")"

  def clause: Parser[Clause] = (predicate | paren) *
    ("and" ^^^ { (a: Clause, b: Clause) => And(a, b) } |
      "or" ^^^ { (a: Clause, b: Clause) => Or(a, b) })

  /** TODO: exception on toInnerVal with wrong type */
  def predicate =
    ident ~ ("=" | "!=") ~ anyStr ^^ {
      case f ~ op ~ s =>
        metaProps.get(f) match {
          case None =>
            throw new RuntimeException(s"where clause contains not existing property name: $f")
          case Some(metaProp) =>
            val eq = if (f == LabelMeta.to.name) {
              Equal(LabelMeta.to.seq, toInnerVal(s, label.tgtColumnType, label.schemaVersion))
            } else if (f == LabelMeta.from.name) {
              Equal(LabelMeta.from.seq, toInnerVal(s, label.srcColumnType, label.schemaVersion))
            } else {
              Equal(metaProp.seq, toInnerVal(s, metaProp.dataType, label.schemaVersion))
            }

            if (op == "=") eq
            else Not(eq)
        }
    } | ident ~ ("between" ~> anyStr <~ "and") ~ anyStr ^^ {
      case f ~ minV ~ maxV =>
        metaProps.get(f) match {
          case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
          case Some(metaProp) =>
            Between(metaProp.seq, toInnerVal(minV, metaProp.dataType, label.schemaVersion),
              toInnerVal(maxV, metaProp.dataType, label.schemaVersion))
        }
    } | ident ~ ("not in" | "in") ~ ("(" ~> rep(anyStr | ",") <~ ")") ^^ {
      case f ~ op ~ vals =>
        metaProps.get(f) match {
          case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
          case Some(metaProp) =>
            val values = vals.filter(v => v != ",").map { v =>
              toInnerVal(v, metaProp.dataType, label.schemaVersion)
            }

            if (op == "in") IN(metaProp.seq, values.toSet)
            else Not(IN(metaProp.seq, values.toSet))
        }
      case _ => throw new RuntimeException(s"failed to parse where clause. ")
    }

  def parse(sql: String): Try[Where] = {
    try {
      parseAll(where, sql) match {
        case Success(r, q) => scala.util.Success(r)
        case fail => scala.util.Failure(new RuntimeException(s"sql parsing error: ${fail.toString}"))
      }
    } catch {
      case ex: Exception => scala.util.Failure(ex)
    }
  }
}
