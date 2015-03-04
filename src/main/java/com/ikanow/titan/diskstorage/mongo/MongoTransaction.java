package com.ikanow.titan.diskstorage.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;

/**
 * This class overrides and adds nothing compared with
 * {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction}; however, it creates a transaction type specific
 * to Mongo, which lets us check for user errors.
 *
 * @author Joern Freydank
 */
public class MongoTransaction extends AbstractStoreTransaction {
    private static final Logger logger = LoggerFactory.getLogger(MongoTransaction.class);

    public MongoTransaction(final BaseTransactionConfig config) {
        super(config);
    }
    
    @Override
    public void commit() throws StorageException {
    	logger.trace("commit()");
    }

    @Override
    public void rollback() throws StorageException {
    	logger.trace("rollback()");
    }

    @Override
    public BaseTransactionConfig getConfiguration() {
    	logger.trace("getConfiguration()");
       return super.getConfiguration();
    }

}
