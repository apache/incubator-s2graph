package org.apache.s2graph.graphql

import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.schema._

package object types {

  def wrapField(objectName: String, fieldName: String, fields: Seq[Field[GraphRepository, Any]]): Field[GraphRepository, Any] = {
    val tpe = ObjectType(objectName, fields = fields.toList)
    Field(fieldName, tpe, resolve = c => c.value): Field[GraphRepository, Any]
  }

  def toScalarType(from: String): ScalarType[_] = from match {
    case "string" => StringType
    case "int" => IntType
    case "integer" => IntType
    case "long" => LongType
    case "float" => FloatType
    case "double" => FloatType
    case "boolean" => BooleanType
    case "bool" => BooleanType
  }
}
