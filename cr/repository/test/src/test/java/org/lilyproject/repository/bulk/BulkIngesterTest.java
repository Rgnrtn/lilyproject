/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.repository.bulk;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.lilyproject.repository.api.BlobReference;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEvent.Type;

public class BulkIngesterTest {

    private HBaseRepository hbaseRepository;
    private HTableInterface recordTable;
    private FieldTypes fieldTypes;

    private BulkIngester bulkIngester;

    @Before
    public void setUp() {
        hbaseRepository = mock(HBaseRepository.class);
        recordTable = mock(HTableInterface.class);
        fieldTypes = mock(FieldTypes.class);
        bulkIngester = spy(new BulkIngester(hbaseRepository, recordTable, fieldTypes));
    }

    /**
     * Configure the mocks so that the given Put will be returned for the given Record.
     */
    private void configurePutCreation(Record record, Put put) throws InterruptedException, RepositoryException {
        RecordEvent expectedRecordEvent = new RecordEvent();
        expectedRecordEvent.setType(Type.CREATE);
        when(hbaseRepository.buildPut(record, 1L, expectedRecordEvent, fieldTypes,
                        Sets.<BlobReference> newHashSet(), Sets.<BlobReference> newHashSet())).thenReturn(put);
    }

    @Test
    public void testIngest() throws InterruptedException, RepositoryException, IOException {
        Record record = mock(Record.class);
        RecordId recordId = mock(RecordId.class);
        when(record.getId()).thenReturn(recordId);
        Put expectedPut = mock(Put.class);

        configurePutCreation(record, expectedPut);

        bulkIngester.write(record);

        verify(recordTable).put(expectedPut);
        verify(record, never()).setId(any(RecordId.class));

    }

    @Test
    public void testIngest_NoIdOnRecord() throws InterruptedException, RepositoryException, IOException {
        Record record = mock(Record.class);
        Put expectedPut = mock(Put.class);

        RecordId recordId = mock(RecordId.class);
        IdGenerator idGenerator = mock(IdGenerator.class);
        when(idGenerator.newRecordId()).thenReturn(recordId);
        when(hbaseRepository.getIdGenerator()).thenReturn(idGenerator);

        configurePutCreation(record, expectedPut);

        bulkIngester.write(record);

        verify(recordTable).put(expectedPut);
        verify(record).setId(recordId);

    }

}
