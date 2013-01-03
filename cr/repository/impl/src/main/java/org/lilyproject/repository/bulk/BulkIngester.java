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

import java.io.Closeable;
import java.io.IOException;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.lilyproject.repository.api.BlobReference;
import org.lilyproject.repository.api.FieldTypes;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.HBaseRepository;
import org.lilyproject.repository.impl.HBaseTypeManager;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.util.exception.ExceptionUtil;
import org.lilyproject.util.hbase.HBaseTableFactory;
import org.lilyproject.util.hbase.HBaseTableFactoryImpl;
import org.lilyproject.util.hbase.LilyHBaseSchema;
import org.lilyproject.util.repo.RecordEvent;
import org.lilyproject.util.repo.RecordEvent.Type;
import org.lilyproject.util.zookeeper.ZkUtil;
import org.lilyproject.util.zookeeper.ZooKeeperItf;

/**
 * Writes Lily records in bulk to HBase. Also provides methods for creating HBase {@code Put} or
 * {@code KeyValue} object for alternative methods of writing (e.g. via MapReduce).
 */
public class BulkIngester implements Closeable {

    private HBaseRepository hbaseRepo;
    private HTableInterface recordTable;
    private FieldTypes fieldTypes;

    /**
     * Factory method for creation of a {@code BulkIngester}.
     * 
     * @param zkConnString Connection string for ZooKeeper
     * @param timeout ZooKeeper session timeout
     * @return a new BulkIngester
     */
    public static BulkIngester newBulkIngester(String zkConnString, int timeout) {
        try {
            ZooKeeperItf zk = ZkUtil.connect(zkConnString, timeout);
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zkConnString);
            HBaseTableFactory hbaseTableFactory = new HBaseTableFactoryImpl(conf);
            IdGenerator idGenerator = new IdGeneratorImpl();
            TypeManager typeManager = new HBaseTypeManager(idGenerator, conf, zk, hbaseTableFactory);

            // FIXME Blobs aren't really supported here (no BlobManager is created), but null is
            // just passed in as the BlobManager so we'll probably get some mystery NPEs if Blobs
            // are used. We should either support blobs, or at least forcefully disallow them
            HBaseRepository hbaseRepository = new HBaseRepository(typeManager, idGenerator, null, hbaseTableFactory,
                    null, null);
            return new BulkIngester(hbaseRepository, LilyHBaseSchema.getRecordTable(hbaseTableFactory),
                    typeManager.getFieldTypesSnapshot());
        } catch (Exception e) {
            ExceptionUtil.handleInterrupt(e);
            throw new RuntimeException(e);
        }
    }

    BulkIngester(HBaseRepository hbaseRepo, HTableInterface recordTable, FieldTypes fieldTypes) {
        this.hbaseRepo = hbaseRepo;
        this.recordTable = recordTable;
        this.fieldTypes = fieldTypes;
    }

    /**
     * Factory method for creation of Records, with the same semantics as
     * {@link Repository#newRecord()}.
     * 
     * @return A newly-created record
     */
    public Record newRecord() {
        return hbaseRepo.newRecord();
    }

    /**
     * Same as {@link Repository#getIdGenerator()}.
     * 
     * @return The IdGenerator of the underlying repository
     */
    public IdGenerator getIdGenerator() {
        return hbaseRepo.getIdGenerator();
    }

    /**
     * Write a single record directly to Lily, circumventing any indexing or other secondary actions
     * that are performed when using the standard Lily API.
     * 
     * @param record Record to be written
     * @throws IOException
     */
    public void write(Record record) throws InterruptedException, RepositoryException, IOException {
        recordTable.put(buildPut(record));
    }

    /**
     * Build the {@code Put} that represents a record for inserting into HBase.
     * @param record The record to be translated into an HBase {@code Put}
     * @return Put which can be directly written to HBase
     */
    public Put buildPut(Record record) throws InterruptedException, RepositoryException {
        RecordEvent recordEvent = new RecordEvent();
        recordEvent.setType(Type.CREATE);
        if (record.getId() == null) {
            record.setId(getIdGenerator().newRecordId());
        }
        return hbaseRepo.buildPut(record, 1L, recordEvent, fieldTypes, Sets.<BlobReference> newHashSet(),
                Sets.<BlobReference> newHashSet());
    }
    
    @Override
    public void close() throws IOException {
        hbaseRepo.close();
    }

}
