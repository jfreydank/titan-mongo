package com.ikanow.titan.diskstorage.mongo;

import com.google.common.collect.Sets;
import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

public class MongoStorageSetup extends StorageSetup {

    public static ModifiableConfiguration getMongoConfiguration() {
    	   return buildConfiguration()
                   .set(STORAGE_BACKEND,"mongo")
    	   			.set(STORAGE_HOSTS,new String[]{"127.0.0.1"});
    }

    public static WriteConfiguration getMongoGraphConfiguration() {
        return getMongoConfiguration().getConfiguration();
    }

    public static ModifiableConfiguration getMongoPerformanceConfiguration() {
        return getMongoConfiguration()
                .set(STORAGE_TRANSACTIONAL,false)
                .set(TX_CACHE_SIZE,1000);
    }
}
