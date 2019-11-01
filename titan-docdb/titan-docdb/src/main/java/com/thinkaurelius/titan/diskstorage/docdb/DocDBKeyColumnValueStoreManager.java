package com.thinkaurelius.titan.diskstorage.docdb;

import com.google.common.base.Preconditions;
import com.microsoft.azure.documentdb.*;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import javassist.NotFoundException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jmondal on 6/28/2016.
 */
public class DocDBKeyColumnValueStoreManager extends LocalStoreManager implements KeyColumnValueStoreManager {

    protected final StoreFeatures features;

    private final Map<String, DocDBKeyColumnValueStore> stores;
    private static final String END_POINT = "https://localhost:443/";
    private static final String DATABASE_ID = "DocDBStorageForTitan";
    private static final String MASTER_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
    private static final com.microsoft.azure.documentdb.DocumentClient documentClient = new DocumentClient(END_POINT, MASTER_KEY, ConnectionPolicy.GetDefault(), com.microsoft.azure.documentdb.ConsistencyLevel.Session);;
    private static Database database = null;
    private static boolean isOpen;

    public DocDBKeyColumnValueStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new HashMap<String, DocDBKeyColumnValueStore>();

        if(this.database == null) {
            try {

                this.database = new com.microsoft.azure.documentdb.Database();
                this.database.setId(DATABASE_ID);

                this.database = documentClient.createDatabase(this.database, null)
                        .getResource();
            } catch (DocumentClientException e) {

                try {
                    this.database = documentClient.readDatabase("dbs/" + DATABASE_ID, null).getResource();
                }
                catch (Exception ex) {
                    System.out.print("Could Not create Database" + DATABASE_ID);
                    throw new PermanentBackendException("Could not open DocDB data store", ex);
                }
            }
        }


        // ToDo: Carefully evaluate this features
        features = new StandardStoreFeatures.Builder()
                .orderedScan(true)
                .transactional(false)
                .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                .locking(true)
                .keyOrdered(true)
                .timestamps(false)
                .multiQuery(true)
                .batchMutation(true)
                .build();
    }

    public Database getDatabase()
    {
        return this.database;
    }

    @Override
    public DocDBKeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            DocDBKeyColumnValueStore store = stores.get(name);
            return store;
        }
        // ToDo: Consider passing DBConfig, and throw exception on failure

        DocDBKeyColumnValueStore store = new DocDBKeyColumnValueStore(name, this, documentClient);
        stores.put(name, store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {

        Map<StaticBuffer, KCVMutation> mutationMap;

        for(String storeName: mutations.keySet())
        {
            if(!stores.containsKey(storeName))
            {
                throw new PermanentBackendException("Could not find store: "+storeName);
            }

            mutationMap = mutations.get(storeName);
            for(StaticBuffer key: mutationMap.keySet())
            {
                stores.get(storeName).mutate(key, mutationMap.get(key).getAdditions(), mutationMap.get(key).getDeletions(), txh);
            }
        }
    }

    @Override
    public DocDBStoreTransaction beginTransaction(BaseTransactionConfig config)  {
        DocDBStoreTransaction txh = new DocDBStoreTransaction(config);
        return txh;
    }

    @Override
    public void close() throws BackendException {
            // do nothing
    }

    @Override
    public void clearStorage() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getName() {
        return "DocDbKeyColumnValueSTore";
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }
}
