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
package org.lilycms.repository.api;

/**
 *
 */
public class FieldGroupExistsException extends Exception {

    private final FieldGroup fieldGroup;

    public FieldGroupExistsException(FieldGroup fieldGroup) {
        this.fieldGroup = fieldGroup;
    }

    public FieldGroup getFieldGroup() {
        return fieldGroup;
    }
    
    @Override
    public String getMessage() {
        StringBuilder message = new StringBuilder();
        message.append("FieldGroup <");
        message.append(fieldGroup.getId());
        message.append("> ");
        message.append("already exists");
        return message.toString();
    }
}
