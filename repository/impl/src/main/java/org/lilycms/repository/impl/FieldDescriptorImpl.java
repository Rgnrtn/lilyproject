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
package org.lilycms.repository.impl;

import org.lilycms.repository.api.FieldDescriptor;
import org.lilycms.repository.api.TypeManager;
import org.lilycms.repository.api.ValueType;

public class FieldDescriptorImpl implements FieldDescriptor {

    private final String id;
    private Long version;
    private ValueType valueType;
    private String globalName;

    /**
     * This constructor should not be called directly.
     * @use {@link TypeManager#newFieldDescriptor} instead
     */
    public FieldDescriptorImpl(String id, ValueType valueType, String globalName) {
        this.id = id;
        this.valueType = valueType;
        this.globalName = globalName;
    }

    public String getGlobalName() {
        return globalName;
    }

    public String getId() {
        return id;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public Long getVersion() {
        return version;
    }

    public void setGlobalName(String name) {
        this.globalName = name;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    public void setVersion(Long version) {
        this.version = version;
    }
    
    public FieldDescriptor clone() {
        FieldDescriptorImpl clone = new FieldDescriptorImpl(this.id, this.valueType, this.globalName);
        clone.version = this.version;
        return clone;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((globalName == null) ? 0 : globalName.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((valueType == null) ? 0 : valueType.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FieldDescriptorImpl other = (FieldDescriptorImpl) obj;
        if (globalName == null) {
            if (other.globalName != null)
                return false;
        } else if (!globalName.equals(other.globalName))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (valueType == null) {
            if (other.valueType != null)
                return false;
        } else if (!valueType.equals(other.valueType))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "FieldDescriptorImpl [id=" + id + ", version=" + version + ", globalName=" + globalName
                        + ", valueType=" + valueType + "]";
    }
}
