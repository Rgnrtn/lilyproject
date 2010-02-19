package org.lilycms.hbaseindex.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.lilycms.hbaseindex.*;
import org.lilycms.testfw.TestHelper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;

public class IndexTest {
    private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TestHelper.setupLogging();
        TEST_UTIL.startMiniCluster(1);

        IndexManager.createIndexMetaTable(TEST_UTIL.getConfiguration());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    @Test
    public void testSingleStringFieldIndex() throws Exception {
        final String INDEX_NAME = "singlestringfield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME);

        // Create a few index entries, inserting them in non-sorted order
        String[] values = {"d", "a", "c", "e", "b"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));            
        }

        Query query = new Query();
        query.setRangeCondition("field1", "b", "d");
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey4", Bytes.toString(result.next()));
        assertEquals("targetkey2", Bytes.toString(result.next()));
        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testSingleByteFieldIndex() throws Exception {
        final String INDEX_NAME = "singlebytefield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        ByteIndexFieldDefinition fieldDef = indexDef.addByteField("field1");
        fieldDef.setLength(3);
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME);

        // Create a few index entries, inserting them in non-sorted order
        byte[][] values = {Bytes.toBytes("aaa"), Bytes.toBytes("aab")};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", Bytes.toBytes("aaa"), Bytes.toBytes("aab"));
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertEquals("targetkey1", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testSingleIntFieldIndex() throws Exception {
        final String INDEX_NAME = "singleintfield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addIntegerField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME);

        final int COUNT = 1000;
        final int MAXVALUE = Integer.MAX_VALUE;
        int[] values = new int[COUNT];

        for (int i = 0; i < COUNT; i++) {
            values[i] = (int)(Math.random() * MAXVALUE);
        }

        for (int value : values) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", value);
            index.addEntry(entry, Bytes.toBytes("targetkey" + value));
        }

        Query query = new Query();
        query.setRangeCondition("field1", new Integer(0), new Integer(MAXVALUE));
        QueryResult result = index.performQuery(query);

        Arrays.sort(values);

        for (int value : values) {
            assertEquals("targetkey" + value, Bytes.toString(result.next()));
        }

        assertNull(result.next());
    }

    @Test
    public void testSingleLongFieldIndex() throws Exception {
        final String INDEX_NAME = "singlelongfield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addLongField("field1");
        indexManager.createIndex(indexDef);
        Index index = indexManager.getIndex(INDEX_NAME);

        long values[] = {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE};
        for (long value : values) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", value);
            index.addEntry(entry, Bytes.toBytes("targetkey" + value));
        }

        Query query = new Query();
        query.setRangeCondition("field1", Long.MIN_VALUE, Long.MAX_VALUE);
        QueryResult result = index.performQuery(query);

        for (long value : values) {
            assertEquals("targetkey" + value, Bytes.toString(result.next()));
        }

        assertNull(result.next());
    }

    @Test
    public void testSingleFloatFieldIndex() throws Exception {
        final String INDEX_NAME = "singlefloatfield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addFloatField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        float[] values = {55.45f, 63.88f, 55.46f, 55.47f, -0.3f};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", new Float(55.44f), new Float(55.48f));
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertEquals("targetkey2", Bytes.toString(result.next()));
        assertEquals("targetkey3", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testSingleDateTimeFieldIndex() throws Exception {
        final String INDEX_NAME = "singledatetimefield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        DateTimeIndexFieldDefinition fieldDef = indexDef.addDateTimeField("field1");
        fieldDef.setPrecision(DateTimeIndexFieldDefinition.Precision.DATETIME_NOMILLIS);
        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        DateTimeFormatter formatter = ISODateTimeFormat.basicDateTimeNoMillis();
        Date[] values = {
                formatter.parseDateTime("20100215T140500Z").toDate(),
                formatter.parseDateTime("20100215T140501Z").toDate(),
                formatter.parseDateTime("20100216T100000Z").toDate(),
                formatter.parseDateTime("20100217T100000Z").toDate()
        };

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", formatter.parseDateTime("20100215T140500Z").toDate(),
                formatter.parseDateTime("20100215T140501Z").toDate());
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertEquals("targetkey1", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testSingleDecimalFieldIndex() throws Exception {
        final String INDEX_NAME = "singledecimalfield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addDecimalField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        String[] values = {"33.66", "-1", "-3.00007E77"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", new BigDecimal(values[i]));
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", new BigDecimal(values[2]), new BigDecimal(values[0]));
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey2", Bytes.toString(result.next()));
        assertEquals("targetkey1", Bytes.toString(result.next()));
        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertNull(result.next());

        query = new Query();
        query.addEqualsCondition("field1", new BigDecimal(values[2]));
        result = index.performQuery(query);
        assertEquals("targetkey2", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testDuplicateValuesIndex() throws Exception {
        final String INDEX_NAME = "duplicatevalues";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        // Create a few index entries, inserting them in non-sorted order
        String[] values = {"a", "a", "a", "a", "b", "c", "d"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.addEqualsCondition("field1", "a");
        QueryResult result = index.performQuery(query);

        assertResultSize(4, result);
    }

    private void assertResultSize(int expectedCount, QueryResult result) throws IOException {
        int matchCount = 0;
        while (result.next() != null) {
            matchCount++;
        }
        assertEquals(expectedCount, matchCount);
    }

    @Test
    public void testMultiFieldIndex() throws Exception {
        final String INDEX_NAME = "multifield";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addIntegerField("field1");
        indexDef.addStringField("field2");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        IndexEntry entry = new IndexEntry();
        entry.addField("field1", 10);
        entry.addField("field2", "a");
        index.addEntry(entry, Bytes.toBytes("targetkey1"));
        index.addEntry(entry, Bytes.toBytes("targetkey2"));
        index.addEntry(entry, Bytes.toBytes("targetkey3"));

        entry = new IndexEntry();
        entry.addField("field1", 11);
        entry.addField("field2", "a");
        index.addEntry(entry, Bytes.toBytes("targetkey4"));

        entry = new IndexEntry();
        entry.addField("field1", 10);
        entry.addField("field2", "b");
        index.addEntry(entry, Bytes.toBytes("targetkey5"));

        Query query = new Query();
        query.addEqualsCondition("field1", 10);
        query.addEqualsCondition("field2", "a");
        QueryResult result = index.performQuery(query);

        assertResultSize(3, result);
    }

    @Test
    public void testDeleteFromIndex() throws Exception {
        final String INDEX_NAME = "deletefromindex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        // Add the entry
        IndexEntry entry = new IndexEntry();
        entry.addField("field1", "foobar");
        index.addEntry(entry, Bytes.toBytes("key1"));

        // Test it is there
        Query query = new Query();
        query.addEqualsCondition("field1", "foobar");
        QueryResult result = index.performQuery(query);
        assertEquals("key1", Bytes.toString(result.next()));
        assertNull(result.next());

        // Delete the entry
        index.removeEntry(entry, Bytes.toBytes("key1"));

        // Test it is gone
        result = index.performQuery(query);
        assertNull(result.next());

        // Delete the entry again, this should not give an error
        index.removeEntry(entry, Bytes.toBytes("key1"));
    }

    @Test
    public void testNullIndex() throws Exception {
        final String INDEX_NAME = "nullindex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");
        indexDef.addStringField("field2");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        IndexEntry entry = new IndexEntry();
        entry.addField("field1", "foobar");
        index.addEntry(entry, Bytes.toBytes("key1"));

        entry = new IndexEntry();
        index.addEntry(entry, Bytes.toBytes("key2"));

        entry = new IndexEntry();
        entry.addField("field2", "foobar");
        index.addEntry(entry, Bytes.toBytes("key3"));

        Query query = new Query();
        query.addEqualsCondition("field1", "foobar");
        query.addEqualsCondition("field2", null);
        QueryResult result = index.performQuery(query);
        assertEquals("key1", Bytes.toString(result.next()));
        assertNull(result.next());

        query = new Query();
        query.addEqualsCondition("field1", null);
        query.addEqualsCondition("field2", null);
        result = index.performQuery(query);
        assertEquals("key2", Bytes.toString(result.next()));
        assertNull(result.next());

        query = new Query();
        query.addEqualsCondition("field1", null);
        query.addEqualsCondition("field2", "foobar");
        result = index.performQuery(query);
        assertEquals("key3", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    @Test
    public void testNotExistingIndex() throws Exception {
        final String INDEX_NAME = "notexisting";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        try {
            indexManager.getIndex(INDEX_NAME);
            fail("Expected an IndexNotFoundException.");
        } catch (IndexNotFoundException e) {
            // ok
        }
    }

    @Test
    public void testDeleteIndex() throws Exception {
        final String INDEX_NAME = "deleteindex";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("foo");
        indexManager.createIndex(indexDef);

        indexManager.getIndex(INDEX_NAME);

        indexManager.deleteIndex(INDEX_NAME);

        try {
            indexManager.getIndex(INDEX_NAME);
            fail("Expected an IndexNotFoundException.");
        } catch (IndexNotFoundException e) {
            // ok
        }
    }

    @Test
    public void testIndexEntryVerificationIndex() throws Exception {
        final String INDEX_NAME = "indexentryverification";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("stringfield");
        indexDef.addFloatField("floatfield");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        IndexEntry entry = new IndexEntry();
        entry.addField("nonexistingfield", "foobar");

        try {
            index.addEntry(entry, Bytes.toBytes("key"));
            fail("Expected a MalformedIndexEntryException.");
        } catch (MalformedIndexEntryException e) {
            // ok
        }

        entry = new IndexEntry();
        entry.addField("stringfield", new Integer(55));

        try {
            index.addEntry(entry, Bytes.toBytes("key"));
            fail("Expected a MalformedIndexEntryException.");
        } catch (MalformedIndexEntryException e) {
            // ok
        }

        entry = new IndexEntry();
        entry.addField("floatfield", "hello world");

        try {
            index.addEntry(entry, Bytes.toBytes("key"));
            fail("Expected a MalformedIndexEntryException.");
        } catch (MalformedIndexEntryException e) {
            // ok
        }
    }

    @Test
    public void testStringPrefixQuery() throws Exception {
        final String INDEX_NAME = "stringprefixquery";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        String[] values = {"baard", "boer", "beek", "kanaal", "paard"};

        for (int i = 0; i < values.length; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", values[i]);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        Query query = new Query();
        query.setRangeCondition("field1", "b", "b");
        QueryResult result = index.performQuery(query);

        assertEquals("targetkey0", Bytes.toString(result.next()));
        assertEquals("targetkey2", Bytes.toString(result.next()));
        assertEquals("targetkey1", Bytes.toString(result.next()));
        assertNull(result.next());
    }

    /**
     * Test searching on a subset of the fields.
     */
    @Test
    public void testPartialQuery() throws Exception {
        final String INDEX_NAME = "partialquery";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");
        indexDef.addIntegerField("field2");
        indexDef.addStringField("field3");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        for (int i = 0; i < 3; i++) {
            IndexEntry entry = new IndexEntry();
            entry.addField("field1", "value A " + i);
            entry.addField("field2", 10 + i);
            entry.addField("field3", "value B " + i);
            index.addEntry(entry, Bytes.toBytes("targetkey" + i));
        }

        // Search only on the leftmost field
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            QueryResult result = index.performQuery(query);
            assertEquals("targetkey0", Bytes.toString(result.next()));
            assertNull(result.next());
        }

        // Search only on the two leftmost fields
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            query.addEqualsCondition("field2", 10);
            QueryResult result = index.performQuery(query);
            assertEquals("targetkey0", Bytes.toString(result.next()));
            assertNull(result.next());
        }

        // Search only on the two leftmost fields, with range query on the second
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            query.setRangeCondition("field2", 9, 11);
            QueryResult result = index.performQuery(query);
            assertEquals("targetkey0", Bytes.toString(result.next()));
            assertNull(result.next());
        }

        // Try searching on just the second field, should give error
        {
            Query query = new Query();
            query.addEqualsCondition("field2", 10);
            try {
                index.performQuery(query);
                fail("Exception expected");
            } catch (MalformedQueryException e) {
                //System.out.println(e.getMessage());
            }
        }

        // Try searching on just the second field, should give error
        {
            Query query = new Query();
            query.setRangeCondition("field2", 9, 11);
            try {
                index.performQuery(query);
                fail("Exception expected");
            } catch (MalformedQueryException e) {
                //System.out.println(e.getMessage());
            }
        }

        // Try not using all fields from left to right, should give error
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            // skip field 2
            query.addEqualsCondition("field3", "value B 0");
            try {
                index.performQuery(query);
                fail("Exception expected");
            } catch (MalformedQueryException e) {
                //System.out.println(e.getMessage());
            }
        }

        // Try not using all fields from left to right, should give error
        {
            Query query = new Query();
            query.addEqualsCondition("field1", "value A 0");
            // skip field 2
            query.setRangeCondition("field3", "a", "b");
            try {
                index.performQuery(query);
                fail("Exception expected");
            } catch (MalformedQueryException e) {
                //System.out.println(e.getMessage());
            }
        }
    }

    @Test
    public void testDataTypeChecks() throws Exception {
        final String INDEX_NAME = "datatypechecks";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        indexDef.addStringField("field1");
        indexDef.addIntegerField("field2");

        indexManager.createIndex(indexDef);

        Index index = indexManager.getIndex(INDEX_NAME);

        //
        // Index entry checks
        //

        // First test correct situation
        IndexEntry entry = new IndexEntry();
        entry.addField("field1", "a");
        index.addEntry(entry, Bytes.toBytes("1"));

        // Now test incorrect situation
        entry = new IndexEntry();
        entry.addField("field1", 55);
        try {
            index.addEntry(entry, Bytes.toBytes("1"));
            fail("Expected exception.");
        } catch (MalformedIndexEntryException e) {
            //System.out.println(e.getMessage());
        }

        //
        // Query checks
        //

        // First test correct situation
        Query query = new Query();
        query.addEqualsCondition("field1", "a");
        index.performQuery(query);

        // Now test incorrect situation
        query = new Query();
        query.addEqualsCondition("field1", 55);
        try {
            index.performQuery(query);
            fail("Expected exception.");
        } catch (MalformedQueryException e) {
            //System.out.println(e.getMessage());
        }
    }

    @Test
    public void testEmptyDefinition() throws Exception {
        final String INDEX_NAME = "emptydef";
        IndexManager indexManager = new IndexManager(TEST_UTIL.getConfiguration());

        IndexDefinition indexDef = new IndexDefinition(INDEX_NAME);
        try {
            indexManager.createIndex(indexDef);
            fail("Exception expected.");
        } catch (IllegalArgumentException e) {}
    }    
}

