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
package org.lilyproject.repository.impl.valuetype;

import java.util.Comparator;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.IdentityRecordStack;
import org.lilyproject.repository.api.ValueType;
import org.lilyproject.repository.api.ValueTypeFactory;

public class DateValueType extends AbstractValueType implements ValueType {

    public final static String NAME = "DATE";

    private static final Comparator<LocalDate> COMPARATOR = new Comparator<LocalDate>() {
        @Override
        public int compare(LocalDate o1, LocalDate o2) {
            return o1.compareTo(o2);
        }
    };

    @Override
    public String getBaseName() {
        return NAME;
    }

    @Override
    public ValueType getDeepestValueType() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public LocalDate read(DataInput dataInput) {
        // Read the encoding version byte, but ignore it for the moment since there is only one encoding
        dataInput.readByte();
        // Always construct UTC such that it is not depending on the default timezone of the current host
        return new LocalDate(dataInput.readLong(), DateTimeZone.UTC);
    }

    @Override
    public void write(Object value, DataOutput dataOutput, IdentityRecordStack parentRecords) {
        dataOutput.writeByte((byte)1); // Encoding version 1
        // Currently we only store the millis, not the chronology.
        dataOutput.writeLong(((LocalDate)value).toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis());
    }

    @Override
    public Class getType() {
        return LocalDate.class;
    }

    @Override
    public Comparator getComparator() {
        return COMPARATOR;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + NAME.hashCode();
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
        return true;
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory() {
        return new DateValueTypeFactory();
    }
    
    public static class DateValueTypeFactory implements ValueTypeFactory {
        private static DateValueType instance = new DateValueType();
        
        @Override
        public ValueType getValueType(String typeParams) {
            return instance;
        }
    }
}
