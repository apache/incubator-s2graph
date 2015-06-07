package com.daumkakao.s2graph.core.parsers

import com.daumkakao.s2graph.core._
import com.daumkakao.s2graph.core.models.{LabelMeta, Label}
import com.daumkakao.s2graph.core.types2.InnerValLike

import scala.util.parsing.combinator.JavaTokenParsers

/**
 * Created by shon on 5/30/15.
 */
case class Where(val clauses: Seq[Clause] = Seq.empty[Clause]) {
  def filter(edge: Edge): Boolean = {
    clauses.map(_.filter(edge)).forall(r => r)
  }
}

abstract class Clause {
  def and(otherField: Clause): Clause = And(this, otherField)
  def or(otherField: Clause): Clause = Or(this, otherField)
  def filter(edge: Edge): Boolean = ???
}
case class Equal(val propKey: Byte, val value: InnerValLike) extends Clause {
  override def filter(edge: Edge): Boolean = {
    propKey match {
      case LabelMeta.from.seq => edge.srcVertex.innerId == value
      case LabelMeta.to.seq =>
        val log = List(edge.tgtVertex.schemaVer,
          edge.tgtVertex.innerId, " vs ", value,
          edge.tgtVertex.innerId.bytes.toList,
          value.bytes.toList
        )
        println(log.mkString("\t"), "\n")
        edge.tgtVertex.innerId == value
      case _ =>
        edge.props.get(propKey) match {
          case None => true
          case Some(edgeVal) => edgeVal == value
        }
    }

  }
}
case class IN(val propKey: Byte, val values: Set[InnerValLike]) extends Clause {
  override def filter(edge: Edge): Boolean = {
    propKey match {
      case LabelMeta.from.seq => values.contains(edge.srcVertex.innerId)
      case LabelMeta.to.seq => values.contains(edge.tgtVertex.innerId)
      case _ =>
        edge.props.get(propKey) match {
          case None => true
          case Some(edgeVal) => values.contains(edgeVal)
        }
    }
  }
}
case class Between(val propKey: Byte, val minValue: InnerValLike, val maxValue: InnerValLike) extends Clause {
  override def filter(edge: Edge): Boolean = {
    propKey match {
      case LabelMeta.from.seq => minValue <= edge.srcVertex.innerId && edge.srcVertex.innerId <= maxValue
      case LabelMeta.to.seq => minValue <= edge.tgtVertex.innerId && edge.tgtVertex.innerId <= maxValue
      case _ =>
        edge.props.get(propKey) match {
          case None => true
          case Some(edgeVal) =>
            minValue <= edgeVal && edgeVal <= maxValue
        }
    }

  }
}
case class Not(val self: Clause) extends Clause {
  override def filter(edge: Edge): Boolean = {
    !self.filter(edge)
  }
}
case class And(val left: Clause, val right: Clause) extends Clause {
  override def filter(edge: Edge): Boolean = {
    left.filter(edge) && right.filter(edge)
  }
}
case class Or(val left: Clause, val right: Clause) extends Clause {
  override def filter(edge: Edge): Boolean = {
    left.filter(edge) || right.filter(edge)
  }
}
case class WhereParser(label: Label) extends JavaTokenParsers with JSONParser {

  val metaProps = label.metaPropsInvMap ++ Map(LabelMeta.from.name -> LabelMeta.from, LabelMeta.to.name -> LabelMeta.to)

  def where: Parser[Where] = rep(clause) ^^ (Where(_))

  def clause: Parser[Clause] = (predicate | parens) * (
    "and" ^^^ { (a: Clause, b: Clause) => And(a, b) } |
      "or" ^^^ { (a: Clause, b: Clause) => Or(a, b) })

  def parens: Parser[Clause] = "(" ~> clause <~ ")"

  def boolean = ("true" ^^^ (true) | "false" ^^^ (false))

  def stringLiteralWithMinus = (stringLiteral | wholeNumber | ("-" ~ stringLiteral | floatingPointNumber) ^^ {
    case _ ~ v => "-" + v
  })

  /** floating point is not supported yet **/
  def predicate = (
    (ident ~ "=" ~ ident | ident ~ "=" ~ decimalNumber | ident ~ "=" ~ stringLiteralWithMinus) ^^ {
      case f ~ "=" ~ s =>
        metaProps.get(f) match {
          case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
          case Some(metaProp) =>
            Equal(metaProp.seq, toInnerVal(s, metaProp.dataType, label.version))
        }
    }
      | (ident ~ "between" ~ ident ~ "and" ~ ident |
         ident ~ "between" ~ decimalNumber ~ "and" ~ decimalNumber |
         ident ~ "between" ~ stringLiteralWithMinus ~ "and" ~ stringLiteralWithMinus) ^^ {
      case f ~ "between" ~ minV ~ "and" ~ maxV =>
        metaProps.get(f) match {
          case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
          case Some(metaProp) =>
            Between(metaProp.seq, toInnerVal(minV, metaProp.dataType, label.version), toInnerVal(maxV, metaProp.dataType, label.version))
        }
    }
      | (ident ~ "in" ~ "(" ~ rep(ident | decimalNumber | stringLiteralWithMinus | "true" | "false" | ",") ~ ")") ^^ {
      case f ~ "in" ~ "(" ~ vals ~ ")" =>
        metaProps.get(f) match {
          case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
          case Some(metaProp) =>
            val values = vals.filter(v => v != ",").map { v =>
              toInnerVal(v, metaProp.dataType, label.version)
            }
            IN(metaProp.seq, values.toSet)
        }
    }
      | (ident ~ "!=" ~ ident | ident ~ "!=" ~ decimalNumber | ident ~ "!=" ~ stringLiteralWithMinus) ^^ {
      case f ~ "!=" ~ s =>
        metaProps.get(f) match {
          case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
          case Some(metaProp) =>
            Not(Equal(metaProp.seq, toInnerVal(s, metaProp.dataType, label.version)))
        }
    }
      | (ident ~ "not in" ~ "(" ~ rep(ident | decimalNumber | stringLiteralWithMinus | "true" | "false" | ",") ~ ")") ^^ {
      case f ~ "not in" ~ "(" ~ vals ~ ")" =>
        metaProps.get(f) match {
          case None => throw new RuntimeException(s"where clause contains not existing property name: $f")
          case Some(metaProp) =>
            val values = vals.filter(v => v != ",").map { v =>
              toInnerVal(v, metaProp.dataType, label.version)
            }
            Not(IN(metaProp.seq, values.toSet))
        }
    }
    )

  def parse(sql: String): Option[Where] = {
    parseAll(where, sql) match {
      case Success(r, q) => Some(r)
      case x => println(x); None
    }
  }
}
