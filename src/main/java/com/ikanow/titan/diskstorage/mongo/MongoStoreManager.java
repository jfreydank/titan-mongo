package com.ikanow.titan.diskstorage.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * In-memory backend storage engine.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class MongoStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(MongoStoreManager.class);
    private final ConcurrentHashMap<String, MongoKeyColumnValueFlatStore> stores;

    private final StoreFeatures features;

    public MongoStoreManager() {
        this(Configuration.EMPTY);
    }

    public static final int PORT_DEFAULT = 27017;

    protected MongoClient mongoClient = null;
    protected DB db = null;
    protected static String DB_NAME="titan";
    
    public MongoStoreManager(final Configuration configuration) {
    	super(configuration, PORT_DEFAULT);
        stores = new ConcurrentHashMap<String, MongoKeyColumnValueFlatStore>();

        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .unorderedScan(true)
            .keyOrdered(true)
            .keyConsistent(GraphDatabaseConfiguration.buildConfiguration())
            .multiQuery(true)
            .build();

//        features = new StoreFeatures();
//        features.supportsOrderedScan = true;
//        features.supportsUnorderedScan = true;
//        features.supportsBatchMutation = false;
//        features.supportsTxIsolation = false;
//        features.supportsConsistentKeyOperations = true;
//        features.supportsLocking = false;
//        features.isDistributed = false;
//        features.supportsMultiQuery = false;
//        features.isKeyOrdered = true;
//        features.hasLocalKeyPartition = false;
        initMongoDB();
       
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws StorageException {
        return new MongoTransaction(config);
    }

    @Override
    public void close() throws StorageException {
        for (MongoKeyColumnValueFlatStore store : stores.values()) {
            store.close();
        }
        stores.clear();
        invalidateMongoDB();    
    }

    /**
     * Deletes and clears all database in this storage manager.
     * <p/>
     * ATTENTION: Invoking this method will delete ALL your data!!
     */
    @Override
    public void clearStorage() throws StorageException {
    	if(mongoClient!=null){
    		mongoClient.dropDatabase(DB_NAME);	
    	}
    	close();
    }
    	
    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public KeyColumnValueStore openDatabase(final String name) throws StorageException {
    	if(mongoClient==null){
    		initMongoDB();
    	}
        if (!stores.containsKey(name)) {
            stores.putIfAbsent(name, new MongoKeyColumnValueFlatStore(db,name));
        }
        KeyColumnValueStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMut : mutations.entrySet()) {
            KeyColumnValueStore store = stores.get(storeMut.getKey());
            
           Preconditions.checkNotNull(store);
            for (Map.Entry<StaticBuffer, KCVMutation> keyMut : storeMut.getValue().entrySet()) {
            	
                logger.debug("Mutating {}", storeMut.getKey());
                store.mutate(keyMut.getKey(), keyMut.getValue().getAdditions(), keyMut.getValue().getDeletions(), txh);
            }
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return toString();
    }

   
    @Override
    public Deployment getDeployment() {
   
    	return Deployment.REMOTE;
    }
    
    protected void initMongoDB() {
    	 try {
 	        List<ServerAddress> seeds = new ArrayList<ServerAddress>();
 	        for (int i = 0; i < hostnames.length; i++) {
 	        	seeds.add(new ServerAddress(hostnames[i],port));
 	        }
 	        
 	        //MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
 	        // MongoClientOptions mongoOptions = builder.build();
 	        //this.mongoClient = new MongoClient(seeds, mongoOptions);
 	        // TODO pass in more config options as parameters
 	        this.mongoClient = new MongoClient(seeds);
 	        this.db = mongoClient.getDB(DB_NAME); 
         }catch(Exception e){
         	logger.error("Error connecting to MongoDB",e);
         }
    }
    
    protected void invalidateMongoDB(){
    	if(mongoClient!=null){
    		mongoClient.close();
    		db=null;
    	}
    }
}
