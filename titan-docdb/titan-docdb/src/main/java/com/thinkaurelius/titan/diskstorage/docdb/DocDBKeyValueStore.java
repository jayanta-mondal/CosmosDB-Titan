package com.thinkaurelius.titan.diskstorage.docdb;

import com.google.common.base.Preconditions;
import com.sleepycat.je.*;
import com.microsoft.azure.documentdb.*;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.SystemConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

public class DocDBKeyValueStore implements OrderedKeyValueStore {

    //private static final Logger log = LoggerFactory.getLogger(DocDBKeyValueStore.class);
    private static final String END_POINT = "https://localhost:443/";
    private static final String MASTER_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    // Define an id for your database and collection
    private static final String DATABASE_ID = "db2";

    private static final StaticBuffer.Factory<DatabaseEntry> ENTRY_FACTORY = new StaticBuffer.Factory<DatabaseEntry>() {
        @Override
        public DatabaseEntry get(byte[] array, int offset, int limit) {
            return new DatabaseEntry(array,offset,limit-offset);
        }
    };


    private final com.sleepycat.je.Database db;
    private final com.microsoft.azure.documentdb.DocumentClient documentClient;
    private final com.microsoft.azure.documentdb.DocumentCollection collection;
    private final String name;
    private final DocDBStoreManager manager;
    private boolean isOpen;
    private final Random rand = new Random();


    public DocDBKeyValueStore(String n, com.sleepycat.je.Database data, DocDBStoreManager m, com.microsoft.azure.documentdb.DocumentClient dc) {
        db = data;
        name = n;
        manager = m;
        isOpen = true;
        documentClient = dc;
        collection = this.CreateCollection(n);
    }

    /*
          This should be moved to create database, instead of open database
     */
    private DocumentCollection CreateCollection(String collName) {

        if(this.collection != null)
        {
            return this.collection;
        }

        com.microsoft.azure.documentdb.Database myDatabase;
        try
        {
            myDatabase = documentClient.readDatabase("dbs/" + DATABASE_ID, null).getResource();
        }
        catch(DocumentClientException ex)
        {
            myDatabase = new com.microsoft.azure.documentdb.Database();
            myDatabase.setId(DATABASE_ID);

            // Create a new database.
            try {
                myDatabase = documentClient.createDatabase(myDatabase, null)
                        .getResource();
            }
            catch(DocumentClientException ex1)
            {
                System.out.print("Could Not create Database"+DATABASE_ID);
            }
        }

        try
        {
            // Define a new collection using the id above.
            DocumentCollection myCollection = new DocumentCollection();
            myCollection.setId(collName);
            Index[] indexArry = new Index[1];
            indexArry[0] = new RangeIndex(DataType.String);
            IndexingPolicy ip = new IndexingPolicy(indexArry);
            myCollection.setIndexingPolicy(ip);

            // Set the provisioned throughput for this collection to be 1000 RUs.
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setOfferThroughput(10000);

            // Create a new collection.
            myCollection = documentClient.createCollection(
                    myDatabase.getSelfLink(), myCollection, requestOptions).getResource();

            return myCollection;
        }
        catch(DocumentClientException ex) {
            try {
                return documentClient.readCollection("dbs/" + DATABASE_ID+"/colls/"+collName, null).getResource();
            } catch (DocumentClientException ex2) {
                return null;
            }
        }
    }

    private String getTitanComparableKeyHexString(StaticBuffer key)
    {
        StringBuilder sb = new StringBuilder();
        ByteBuffer buf = key.asByteBuffer();
        for(int i = buf.position() ; i < buf.limit(); i++)
        {
            sb.append(buf.getChar(i));
        }
        return sb.toString();
    }

    public DatabaseConfig getConfiguration() throws BackendException {
        try {
            return db.getConfig();
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    private static final Transaction getTransaction(StoreTransaction txh) {
        Preconditions.checkArgument(txh!=null);
        return ((DocDBTx) txh).getTransaction();
    }

    @Override
    public synchronized void close() throws BackendException {
        try {
            if(isOpen) db.close();
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
        if (isOpen) manager.removeDatabase(this);
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {

        int k = DocDbUtils.getHashCode(key);

        try {
            String s = this.documentClient.readDocument("dbs/"+DATABASE_ID+"/colls/"+name+"/docs/"+k, null).getResource().getString(k+"");
            StaticBuffer sb = new StaticArrayBuffer(Hex.decodeHex(s.toCharArray()));
            return sb;
        }
        catch (DocumentClientException ex) {
        }
        catch(DecoderException exd)
        {
        }

        return null;

        /*
        Transaction tx = getTransaction(txh);
        try {
            DatabaseEntry dbkey = key.as(ENTRY_FACTORY);
            DatabaseEntry data = new DatabaseEntry();

            log.trace("db={}, op=get, tx={}", name, txh);

            OperationStatus status = db.get(tx, dbkey, data, getLockMode(txh));

            if (status == OperationStatus.SUCCESS) {
                return getBuffer(data);
            } else {
                return null;
            }
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
        */

    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        if (getTransaction(txh) == null) {
            //log.warn("Attempt to acquire lock with transactions disabled");
        } //else we need no locking
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        //log.trace("beginning db={}, op=getSlice, tx={}", name, txh);


        /*
        Transaction tx = getTransaction(txh);
        Cursor cursor = null;
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final KeySelector selector = query.getKeySelector();
        final List<KeyValueEntry> result = new ArrayList<KeyValueEntry>();
        */

        try {

            /*
            DatabaseEntry foundKey = keyStart.as(ENTRY_FACTORY);
            DatabaseEntry foundData = new DatabaseEntry();

            cursor = db.openCursor(tx, null);
            OperationStatus status = cursor.getSearchKeyRange(foundKey, foundData, getLockMode(txh));
            //Iterate until given condition is satisfied or end of records
            while (status == OperationStatus.SUCCESS) {
                StaticBuffer key = getBuffer(foundKey);

                if (key.compareTo(keyEnd) >= 0)
                    break;

                if (selector.include(key)) {
                    result.add(new KeyValueEntry(key, getBuffer(foundData)));
                }

                if (selector.reachedLimit())
                    break;

                status = cursor.getNext(foundKey, foundData, getLockMode(txh));
            }

            // log.trace("db={}, op=getSlice, tx={}, resultcount={}", name, txh, result.size());
            // log.trace("db={}, op=getSlice, tx={}, resultcount={}", name, txh, result.size(), new Throwable("getSlice trace"));
            */

            final List<KeyValueEntry> resultDocDB = new ArrayList<KeyValueEntry>();

            String kStart = DocDbUtils.getTitanComparableKeyHexString(query.getStart());
            String kEnd = DocDbUtils.getTitanComparableKeyHexString(query.getEnd());

            /*
            List<Document> documentList1 = documentClient
                    .queryDocuments("dbs/"+DATABASE_ID+"/colls/"+name,
                            "SELECT * FROM root r",
                            null).getQueryIterable().toList();
                            */

            List<Document> documentList = documentClient
                    .queryDocuments("dbs/"+DATABASE_ID+"/colls/"+name,
                            "select * from c where c.ck between \""+kStart+"\" and \""+kEnd+"\" order by c.ck\n",
                            null).getQueryIterable().toList();

            // De-serialize the documents in to TodoItems.
            String tempKey;
            String tempValue;
            final KeySelector selector = query.getKeySelector();
            int keyOffset, keyLimit, valueOffset, valueLimit;

            for (Document todoItemDocument : documentList) {

                tempKey = todoItemDocument.getString("key");
                keyOffset = todoItemDocument.getInt("keyOffset");
                keyLimit = todoItemDocument.getInt("keyLimit");

                tempValue = todoItemDocument.getString("value");
                valueOffset = todoItemDocument.getInt("valueOffset");
                valueLimit = todoItemDocument.getInt("valueLimit");

                StaticBuffer key = new StaticArrayBuffer(Hex.decodeHex(tempKey.toCharArray()), keyOffset, keyLimit);

                if (selector.include(key)) {
                    resultDocDB.add(new KeyValueEntry(key, new StaticArrayBuffer(Hex.decodeHex(tempValue.toCharArray()), valueOffset, valueLimit)));
                }

                if (selector.reachedLimit()) {
                    break;
                }
            }

            return new RecordIterator<KeyValueEntry>() {
                private final Iterator<KeyValueEntry> entries = resultDocDB.iterator();

                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }

                @Override
                public KeyValueEntry next() {
                    return entries.next();
                }

                @Override
                public void close() {
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        } finally {

            /*
            try {
                if (cursor != null) cursor.close();
            } catch (Exception e) {
                throw new PermanentBackendException(e);
            }
            */
        }
    }

    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        //insert(key, value, txh, true);

        // TODO: Implement Overwrite on duplicate document
        // TODO: Optimize StaticBuffer to Hexcode conversion

        String k = DocDbUtils.getHexString(key);
        String v = DocDbUtils.getHexString(value);


        //String k1 = new String(key.getBytes(0, key.length()), Charset.forName("US-ASCII"));
        //String v1 = new String(value.getBytes(0, value.length()), Charset.forName("US-ASCII"));

        StringBuilder sb = new StringBuilder("{");
        try {

            String comparableKey = DocDbUtils.getTitanComparableKeyHexString(key);
            sb.append("\"ck\":\"").append(comparableKey);
            sb.append("\", \"key\":\"").append(k).append("\", \"keyOffset\":").append(key.asByteBuffer().position()).append(", \"keyLimit\":").append(key.asByteBuffer().limit());
            sb.append(", \"value\":\"");

            if(value != null) {
                if (value.toString() == "") {
                    sb.append("null").append("\", \"valueOffset\":").append("-1").append(", \"valueLimit\":").append("-1");
                } else {
                    sb.append(v).append("\", \"valueOffset\":").append(value.asByteBuffer().position()).append(", \"valueLimit\":").append(value.asByteBuffer().limit());
                }
            }
            else
            {
                sb.append("null");
            }
            sb.append("}");

            Document doc = new Document(sb.toString());
            doc.setId(k.hashCode()+"");
            this.documentClient.upsertDocument(this.collection.getSelfLink(), doc, null, false);
        }
        catch (DocumentClientException ex)
        {
            System.out.println("DocumentClientException: Could not create Doc");
        }
        catch (Exception ex2)
        {
            System.out.println("HMM: Could not create Doc");
        }
    }


    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, boolean allowOverwrite) throws BackendException {
        Transaction tx = getTransaction(txh);
        try {
            OperationStatus status;

            //log.trace("db={}, op=insert, tx={}", name, txh);

            if (allowOverwrite)
                status = db.put(tx, key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));
            else
                status = db.putNoOverwrite(tx, key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));

            if (status != OperationStatus.SUCCESS) {
                if (status == OperationStatus.KEYEXIST) {
                    throw new PermanentBackendException("Key already exists on no-overwrite.");
                } else {
                    throw new PermanentBackendException("Could not write entity, return status: " + status);
                }
            }
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        //log.trace("Deletion");

        int kHash = DocDbUtils.getHashCode(key);

        try {
            // Delete the document by self link.
            documentClient.deleteDocument("dbs/" + DATABASE_ID+"/colls/"+this.collection.getId()+"/docs/"+kHash, null);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        }

        /*
        Transaction tx = getTransaction(txh);
        try {
            //log.trace("db={}, op=delete, tx={}", name, txh);
            OperationStatus status = db.delete(tx, key.as(ENTRY_FACTORY));
            if (status != OperationStatus.SUCCESS) {
                throw new PermanentBackendException("Could not remove: " + status);
            }
        } catch (DatabaseException e) {
            throw new PermanentBackendException(e);
        }
        */
    }

    private static StaticBuffer getBuffer(DatabaseEntry entry) {
        return new StaticArrayBuffer(entry.getData(),entry.getOffset(),entry.getOffset()+entry.getSize());
    }

    private static LockMode getLockMode(StoreTransaction txh) {
        return ((DocDBTx)txh).getLockMode();
    }
}
