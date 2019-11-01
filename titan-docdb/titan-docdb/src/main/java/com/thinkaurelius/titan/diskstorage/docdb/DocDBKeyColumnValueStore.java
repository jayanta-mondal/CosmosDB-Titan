package com.thinkaurelius.titan.diskstorage.docdb;

import com.microsoft.azure.documentdb.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.docdb.Iterator.DocumentKeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.EntryArrayList;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections.map.HashedMap;

import javax.print.Doc;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by jmondal on 6/28/2016.
 */
public class DocDBKeyColumnValueStore extends AbstractDocDbStore{

    private static final int REQUEST_UNIT = 5000;
    private final com.microsoft.azure.documentdb.DocumentClient documentClient;
    private final com.microsoft.azure.documentdb.DocumentCollection collection;
    private final String name;
    private final DocDBKeyColumnValueStoreManager manager;
    private final Random rand = new Random();


    public DocDBKeyColumnValueStore(String storeName, DocDBKeyColumnValueStoreManager storeManager, com.microsoft.azure.documentdb.DocumentClient dc) throws BackendException {
        name = storeName;
        manager = storeManager;
        documentClient = dc;
        collection = this.OpenStore(storeName);
    }

    /*
        Tries to open a store, if not present, creates one.
     */
    private DocumentCollection OpenStore(String collName) throws BackendException {

        if(this.collection != null)
        {
            return this.collection;
        }

        try
        {
            // Define a new collection using the id above.
            DocumentCollection myCollection = new DocumentCollection();
            myCollection.setId(collName);
            Index[] indexArray = new Index[1];
            indexArray[0] = new RangeIndex(DataType.String);
            IndexingPolicy ip = new IndexingPolicy(indexArray);
            myCollection.setIndexingPolicy(ip);

            // Set the provisioned throughput for this collection to be 1000 RUs.
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setOfferThroughput(REQUEST_UNIT);

            // Create a new collection.
            myCollection = documentClient.createCollection(
                    this.manager.getDatabase().getSelfLink(), myCollection, requestOptions).getResource();

            return myCollection;
        }
        catch(DocumentClientException e) {

            try {
                return documentClient.readCollection("dbs/"+this.manager.getDatabase().getId()+"/colls/"+collName, null).getResource();
            } catch (DocumentClientException ex) {
                DocDbUtils.print("Store:" + this.name + " Could not create or find store:" + collName);
                throw new PermanentBackendException("Could not open DocDB data store", ex);
            }
        }
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        List<Entry> entryList = new ArrayList<Entry>();
        String keyHex = DocDbUtils.getTitanComparableKeyHexString(query.getKey());
        String sliceStartHex = DocDbUtils.getTitanComparableKeyHexString(query.getSliceStart());
        String sliceEndHex = DocDbUtils.getTitanComparableKeyHexString(query.getSliceEnd());
        String docDBQueryString = "select * from root where root." + DocDbConstants.COMPARABLE_KEY_STRING+"=\""+keyHex+"\" and ( root." + DocDbConstants.COMPARABLE_COLUMN_STRING + " between \"" + sliceStartHex + "\" and \"" + sliceEndHex+"\" )";
        DocDbUtils.print("Store:" + this.name + " Slice Query: "+ docDBQueryString);

        try {
            List<Document> documentList = documentClient
                    .queryDocuments(this.collection.getSelfLink(),
                            docDBQueryString,
                            null).getQueryIterable().toList();

            // De-serialize the documents in to TodoItems.
            String tempColumn;
            String tempValue;
            int limit = query.hasLimit() ? query.getLimit() : documentList.size();
            int keyOffset, keyLimit, valueOffset, valueLimit;
            int count = 0;

            for (Document todoItemDocument : documentList) {

                tempColumn = todoItemDocument.getString(DocDbConstants.COLUMN_STRING);
                keyOffset = todoItemDocument.getInt(DocDbConstants.COLUMN_OFFSET);
                keyLimit = todoItemDocument.getInt(DocDbConstants.COLUMN_LIMIT);

                tempValue = todoItemDocument.getString(DocDbConstants.VALUE_STRING);
                valueOffset = todoItemDocument.getInt(DocDbConstants.VALUE_OFFSET);
                valueLimit = todoItemDocument.getInt(DocDbConstants.VALUE_LIMIT);
                entryList.add(StaticArrayEntry.of(new StaticArrayBuffer(Hex.decodeHex(tempColumn.toCharArray()), keyOffset, keyLimit), new StaticArrayBuffer(Hex.decodeHex(tempValue.toCharArray()), valueOffset, valueLimit)));

                if(count ++ == limit)
                {
                    break;
                }
            }
        }
        catch (Exception e) {
            throw new PermanentBackendException(e);
        }

        return EntryArrayList.of(entryList);
    }

    public Map<StaticBuffer, EntryList> getSliceParallel(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {

        Map<StaticBuffer, EntryList> map = new HashMap<StaticBuffer, EntryList>();
        ExecutorService executor = Executors.newWorkStealingPool(10);
        Map<StaticBuffer, Future<EntryList>> futures = new HashMap<StaticBuffer, Future<EntryList>>();

        for(StaticBuffer key: keys)
        {
            Future<EntryList> future = executor.submit(() -> {return this.getSlice(new KeySliceQuery(key, query), txh);});
            futures.put(key, future);
        }

        for(StaticBuffer key: futures.keySet())
        {
            try {
                map.put(key, futures.get(key).get());
            } catch (InterruptedException | ExecutionException ex) {
                throw new PermanentBackendException("Parallel Execution Interrupted");
            }
        }

        return map;
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {

        return getSliceParallel(keys, query, txh);

        /*
        Map<StaticBuffer, EntryList> map = new HashMap<StaticBuffer, EntryList>();
        for(StaticBuffer key: keys)
        {
            map.put(key, this.getSlice(new KeySliceQuery(key, query), txh));
        }
        return  map;
        */
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {

        if(additions != null) {
            for (Entry entry : additions) {
                insert(key, entry.getColumn(), entry.getValue());
            }
        }

        if(deletions != null) {
            for (StaticBuffer column : deletions) {
                delete(key, column);
            }
        }
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        //throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {

        String keyStartHex = DocDbUtils.getTitanComparableKeyHexString(query.getKeyStart());
        String keyEndHex = DocDbUtils.getTitanComparableKeyHexString(query.getKeyEnd());
        String sliceStartHex = DocDbUtils.getTitanComparableKeyHexString(query.getSliceStart());
        String sliceEndHex = DocDbUtils.getTitanComparableKeyHexString(query.getSliceEnd());

        String docDBQueryString =  "select root."+DocDbConstants.KEY_STRING+", root." + DocDbConstants.KEY_OFFSET + ", root." + DocDbConstants.KEY_LIMIT
                + " from root where (root." + DocDbConstants.COMPARABLE_KEY_STRING + " between \"" + keyStartHex+"\" and \"" + keyEndHex + "\") and (root."
                + DocDbConstants.COMPARABLE_COLUMN_STRING + " between \"" + sliceStartHex + "\" and \"" + sliceEndHex + "\")";
        int limit = query.hasLimit()?query.getLimit():-1;

        return getKeys(txh, docDBQueryString, limit);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {

        String sliceStartHex = DocDbUtils.getTitanComparableKeyHexString(query.getSliceStart());
        String sliceEndHex = DocDbUtils.getTitanComparableKeyHexString(query.getSliceEnd());
        String docDBQueryString = "select root."+DocDbConstants.KEY_STRING+", root." + DocDbConstants.KEY_OFFSET + ", root." + DocDbConstants.KEY_LIMIT
                +" from root where (root."
                + DocDbConstants.COMPARABLE_COLUMN_STRING + " between \"" + sliceStartHex + "\" and \"" + sliceEndHex + "\")";
        int limit = query.hasLimit()?query.getLimit():-1;

        return getKeys(txh, docDBQueryString, limit);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void close() throws BackendException {

    }

    public KeyIterator getKeys(StoreTransaction txh, String docDbQuery, int queryLimit) throws BackendException {

        DocDbUtils.print("Store:" + this.name + " GetKeys Query:"+ docDbQuery);

        List<Entry> entryList = new ArrayList<Entry>();

        try {
            List<Document> documentList = documentClient
                    .queryDocuments(this.collection.getSelfLink(),
                            docDbQuery,
                            null).getQueryIterable().toList();

            // De-serialize the documents in to TodoItems.
            String key;
            int limit = (queryLimit > 0) ? queryLimit : documentList.size(); // Interface doesn't specify whether we have to respect limit.
            int keyOffset, keyLimit;
            int count = 0;

            for (Document todoItemDocument : documentList) {

                key = todoItemDocument.getString(DocDbConstants.KEY_STRING);
                keyOffset = todoItemDocument.getInt(DocDbConstants.KEY_OFFSET);
                keyLimit = todoItemDocument.getInt(DocDbConstants.KEY_LIMIT);

                entryList.add(StaticArrayEntry.of(new StaticArrayBuffer(Hex.decodeHex(key.toCharArray()), keyOffset, keyLimit)));
            }
        }
        catch (Exception e) {
            throw new PermanentBackendException(e);
        }

        return new DocumentKeyIterator(entryList);
    }

    public void insert(StaticBuffer key, StaticBuffer column, StaticBuffer value) throws BackendException {

        // TODO: Implement Overwrite on duplicate document
        // TODO: Optimize StaticBuffer to Hexcode conversion

        try {
            Document doc = DocDbUtils.geDocument(key, column, value, name);
            this.documentClient.createDocument(this.collection.getSelfLink(), doc, null, false);
        }
        catch (DocumentClientException ex)
        {
            DocDbUtils.print("Store:" + this.name + " DocumentClientException: Could not create Doc");
            throw new PermanentBackendException(ex);
        }
        catch (Exception ex2)
        {
            DocDbUtils.print("Store:" + this.name + "Could not create Doc");
            throw new PermanentBackendException(ex2);
        }
    }

    public void delete(StaticBuffer key, StaticBuffer column) throws BackendException {

        int kHash = DocDbUtils.getHashCode(key);
        int cHash = DocDbUtils.getHashCode(column);

        try {
            documentClient.deleteDocument(this.getDocumentNameBased(kHash+""+cHash), null);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        }
    }

    public String getCollectionNameBased()
    {

        return "dbs/"+this.manager.getDatabase().getId()+"/colls/"+this.collection.getId();
    }

    public String getDocumentNameBased(String docId)
    {
        return this.getCollectionNameBased()+"/docs/"+docId;
    }
}
