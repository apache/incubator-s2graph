
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

package org.apache.s2graph.core.io;



import org.apache.s2graph.core.EdgeId;
import org.apache.s2graph.core.S2VertexPropertyId;
import org.apache.s2graph.core.types.VertexId;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.*;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import java.io.IOException;

public class S2GraphSimpleModule extends SimpleModule {
    private S2GraphSimpleModule() {
        addSerializer(EdgeId.class, new EdgeIdSerializer());
        addSerializer(VertexId.class, new VertexIdSerializer());
        addSerializer(S2VertexPropertyId.class, new S2VertexPropertyIdSerializer());

        addDeserializer(EdgeId.class, new EdgeIdDeserializer());
        addDeserializer(VertexId.class, new VertexIdDeserializer());
        addDeserializer(S2VertexPropertyId.class, new S2VertexPropertyIdDeserializer());
    }

    private static final S2GraphSimpleModule INSTANCE = new S2GraphSimpleModule();

    public static final S2GraphSimpleModule getInstance() {
        return INSTANCE;
    }

    public static class EdgeIdSerializer extends JsonSerializer<EdgeId> {

        @Override
        public void serialize(EdgeId edgeId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
            String s = edgeId.toString();
            jsonGenerator.writeString(s);
        }

        @Override
        public void serializeWithType(EdgeId value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            typeSer.writeCustomTypePrefixForScalar(value, gen, EdgeId.class.getName());
            serialize(value, gen, serializers);
            typeSer.writeCustomTypeSuffixForScalar(value, gen, EdgeId.class.getName());
        }
    }
    public static class EdgeIdDeserializer extends JsonDeserializer<EdgeId> {
        @Override
        public EdgeId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String s = jsonParser.getValueAsString();
            return EdgeId.fromString(s);
        }

        @Override
        public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return typeDeserializer.deserializeTypedFromScalar(p, ctxt);
        }
    }
    public static class VertexIdSerializer extends JsonSerializer<VertexId> {

        @Override
        public void serialize(VertexId vertexId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            String s = vertexId.toString();
            jsonGenerator.writeString(s);
        }

        @Override
        public void serializeWithType(VertexId value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            typeSer.writeCustomTypePrefixForScalar(value, gen, VertexId.class.getName());
            serialize(value, gen, serializers);
            typeSer.writeCustomTypeSuffixForScalar(value, gen, VertexId.class.getName());
        }
    }
    public static class VertexIdDeserializer extends JsonDeserializer<VertexId> {

        @Override
        public VertexId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String s = jsonParser.getValueAsString();
            return VertexId.fromString(s);
        }

        @Override
        public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return typeDeserializer.deserializeTypedFromScalar(p, ctxt);
        }
    }
    public static class S2VertexPropertyIdSerializer extends JsonSerializer<S2VertexPropertyId> {

        @Override
        public void serialize(S2VertexPropertyId s2VertexPropertyId, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
            jsonGenerator.writeString(s2VertexPropertyId.toString());
        }

        @Override
        public void serializeWithType(S2VertexPropertyId value,
                                      JsonGenerator gen,
                                      SerializerProvider serializers, TypeSerializer typeSer) throws IOException {

            typeSer.writeCustomTypePrefixForScalar(value, gen, S2VertexPropertyId.class.getName());
            serialize(value, gen, serializers);
            typeSer.writeCustomTypeSuffixForScalar(value, gen, S2VertexPropertyId.class.getName());
        }
    }
    public static class S2VertexPropertyIdDeserializer extends JsonDeserializer<S2VertexPropertyId> {

        @Override
        public S2VertexPropertyId deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            return S2VertexPropertyId.fromString(jsonParser.getValueAsString());
        }

        @Override
        public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            return typeDeserializer.deserializeTypedFromScalar(p, ctxt);
        }
    }

}
