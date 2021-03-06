/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.tools.import_.json;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.lilyproject.bytes.api.ByteArray;
import org.lilyproject.repository.api.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class RecordWriter implements EntityWriter<Record> {
    public static RecordWriter INSTANCE = new RecordWriter();

    @Override
    public ObjectNode toJson(Record record, WriteOptions options, Repository repository) throws RepositoryException,
            InterruptedException {
        Namespaces namespaces = new NamespacesImpl(options != null ? options.getUseNamespacePrefixes() :
                NamespacesImpl.DEFAULT_USE_PREFIXES);

        ObjectNode recordNode = toJson(record, options, namespaces, repository);

        if (namespaces.usePrefixes()) {
            recordNode.put("namespaces", NamespacesConverter.toJson(namespaces));
        }

        return recordNode;
    }

    @Override
    public ObjectNode toJson(Record record, WriteOptions options, Namespaces namespaces, Repository repository)
            throws RepositoryException, InterruptedException {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode recordNode = factory.objectNode();

        if (record.getId() != null) {
            recordNode.put("id", record.getId().toString());
        }

        if (record.getVersion() != null) {
            recordNode.put("version", record.getVersion());
        }

        if (record.getRecordTypeName() != null) {
            recordNode.put("type", typeToJson(record.getRecordTypeName(), record.getRecordTypeVersion(), namespaces));
        }

        QName versionedTypeName = record.getRecordTypeName(Scope.VERSIONED);
        if (versionedTypeName != null) {
            long version = record.getRecordTypeVersion(Scope.VERSIONED);
            recordNode.put("versionedType", typeToJson(versionedTypeName, version, namespaces));
        }

        QName versionedMutableTypeName = record.getRecordTypeName(Scope.VERSIONED_MUTABLE);
        if (versionedMutableTypeName != null) {
            long version = record.getRecordTypeVersion(Scope.VERSIONED_MUTABLE);
            recordNode.put("versionedMutableType", typeToJson(versionedMutableTypeName, version, namespaces));
        }

        Map<QName, Object> fields = record.getFields();
        if (fields.size() > 0) {
            ObjectNode fieldsNode = recordNode.putObject("fields");

            ObjectNode schemaNode = null;
            if (options.getIncludeSchema()) {
                schemaNode = recordNode.putObject("schema");
            }

            for (Map.Entry<QName, Object> field : fields.entrySet()) {
                FieldType fieldType = repository.getTypeManager().getFieldTypeByName(field.getKey());
                String fieldName = QNameConverter.toJson(fieldType.getName(), namespaces);

                // fields entry
                fieldsNode.put(fieldName, valueToJson(field.getValue(), fieldType.getValueType(), options, namespaces, repository));

                // schema entry
                if (schemaNode != null) {
                    schemaNode.put(fieldName, FieldTypeWriter.toJson(fieldType, namespaces, false));
                }
            }
        }
        
        Map<String, String> attributes = record.getAttributes();
        if (attributes.size() > 0) {
            ObjectNode attributesNode = recordNode.putObject("attributes");
            for (String key : attributes.keySet()) {
                attributesNode.put(key, attributes.get(key));
            }            
        }

        return recordNode;
    }

    private JsonNode listToJson(Object value, ValueType valueType, WriteOptions options, Namespaces namespaces,
            Repository repository) throws RepositoryException, InterruptedException {
        List list = (List)value;
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        for (Object item : list) {
            array.add(valueToJson(item, valueType, options, namespaces, repository));
        }
        return array;
    }

    private JsonNode pathToJson(Object value, ValueType valueType, WriteOptions options, Namespaces namespaces,
            Repository repository) throws RepositoryException, InterruptedException {
        HierarchyPath path = (HierarchyPath)value;
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        for (Object element : path.getElements()) {
            array.add(valueToJson(element, valueType, options, namespaces, repository));
        }
        return array;
    }

    public JsonNode valueToJson(Object value, ValueType valueType, WriteOptions options, Namespaces namespaces,
            Repository repository) throws RepositoryException, InterruptedException {
        String name = valueType.getBaseName();

        JsonNodeFactory factory = JsonNodeFactory.instance;

        JsonNode result;

        if (name.equals("LIST")) {
            result = listToJson(value, valueType.getNestedValueType(), options, namespaces, repository);
        } else if (name.equals("PATH")) {
            result = pathToJson(value, valueType.getNestedValueType(), options, namespaces, repository);
        } else if (name.equals("STRING")) {
            result = factory.textNode((String)value);
        } else if (name.equals("LONG")) {
            result = factory.numberNode((Long)value);
        } else if (name.equals("DOUBLE")) {
            result = factory.numberNode((Double)value);
        } else if (name.equals("BOOLEAN")) {
            result = factory.booleanNode((Boolean)value);
        } else if (name.equals("INTEGER")) {
            result = factory.numberNode((Integer)value);
        } else if (name.equals("URI") || name.equals("DATETIME") || name.equals("DATE") || name.equals("LINK")) {
            result = factory.textNode(value.toString());
        } else if (name.equals("DECIMAL")) {
            result = factory.numberNode((BigDecimal)value);
        } else if (name.equals("BLOB")) {
            Blob blob = (Blob)value;
            result = BlobConverter.toJson(blob);
        } else if (name.equals("RECORD")){
            result = toJson((Record)value, options, namespaces, repository);
        } else if (name.equals("BYTEARRAY")) {
            result = factory.binaryNode(((ByteArray) value).getBytes());
        } else {
            throw new RuntimeException("Unsupported value type: " + name);
        }

        return result;
    }

    private static JsonNode typeToJson(QName name, Long version, Namespaces namespaces) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode jsonType = factory.objectNode();

        jsonType.put("name", QNameConverter.toJson(name, namespaces));
        if (version != null)
            jsonType.put("version", version);

        return jsonType;
    }


}
