package com.ikanow.titan.diskstorage.mongo;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyIterator;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRangeQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;

/**
 * A MongoDB implementation of {@link KeyColumnValueStore}.
 *
 * @author Joern Freydank jfreydank@ikanow.com
 */

public class MongoKeyColumnValueFlatStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoKeyColumnValueFlatStore.class);

    private final String name;
    private final ConcurrentNavigableMap<StaticBuffer, ColumnValueStore> kcv;
    protected DB db = null;
    protected DBCollection dbCollection = null;
    
    public MongoKeyColumnValueFlatStore(@Nonnull DB db,final String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = name;
        this.db = db;
        this.dbCollection=db.getCollection(name);
        this.kcv = new ConcurrentSkipListMap<StaticBuffer, ColumnValueStore>();
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
    	//logger.debug("getSlice S:{} K:{},start:{} end;{}",name,MongoConversionUtil.staticBuffer2Bytes(query.getKey()),MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart()),MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd()));
    	EntryList slice = null;

    	//mongo
    	DBObject dbQuery = MongoQueryBuilder.createKeySliceQuery(query) ; 
        DBCursor dbCursor = dbCollection.find(dbQuery);
        int limit = query.getLimit();
        if(limit > 0){
        	dbCursor.limit(limit);
        }
        slice = MongoQueryBuilder.convertMongoCursorToEntryList(dbCursor);
        
    	logger.trace("getSlice S:{} K:{},start:{} end;{}={}",name,MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getKey()),MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart()),MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd()),slice.size());
        return slice;
    }
    
    

	@Override
	public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh)
			throws StorageException {
		logger.debug("getSlice S:{} Ks:<map>,start:{} end;{}", name,
				MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart()),
				MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd()));

		int limit = query.getLimit();
		Map<StaticBuffer, EntryList> resultMap = Maps.newHashMap();
		if (limit == SliceQuery.NO_LIMIT) {
			DBObject dbQuery = MongoQueryBuilder.createKeysSliceQuery(keys, query);
			DBCursor dbCursor = dbCollection.find(dbQuery);
			dbCursor.limit(limit);
			resultMap = MongoQueryBuilder.convertMongoCursorToEntryMap(dbCursor);
		} else {

			// if limit use getSlice(key, ...) function since we cannot limit
			// per key.
			for (StaticBuffer key : keys) {
				resultMap.put(key, getSlice(new KeySliceQuery(key, query), txh));
			}
		}
		return resultMap;
	}

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
    	
    	//byte[] entryKey = MongoConversionUtil.staticBuffer2Bytes(key); 
    	String entryKeyId = MongoConversionUtil.staticBuffer2MongoFieldNameHex(key);
        logger.trace("mutate:S:{} E:{}",name,entryKeyId);
        
        // mongo       
        
        for (StaticBuffer deletion : deletions) {
            String delColumn = MongoConversionUtil.staticBuffer2MongoFieldNameHex(deletion);
            DBObject queryObject =  MongoQueryBuilder.createKeyColumnQuery(key, deletion);
            Object removedValue = dbCollection.remove(queryObject);
            logger.trace("mutate:S:{} E:{} D:{} V{}:",name,entryKeyId,delColumn,removedValue);
        }
      	    

        if (!additions.isEmpty()) {
    	    // 2. Unordered bulk operation - no guarantee of order of operation
    	    BulkWriteOperation builder = dbCollection.initializeUnorderedBulkOperation();
    	    //builder.find(new BasicDBObject("_id", 1)).removeOne();
    	    //builder.find(new BasicDBObject("_id", 2)).removeOne();
            for (Entry e : additions) {      
            	StaticBuffer cb = e.getColumn(); 
            	String addColumn = MongoConversionUtil.staticBuffer2MongoFieldNameHex(cb);
            	String addColumnDebug = MongoConversionUtil.staticBuffer2String(cb);
            	String addValueDebug = MongoConversionUtil.staticBuffer2String(e.getValue());
    			logger.trace("mutate:S:{} E:{} Converted StaticBuffer1 for {}={} ,{}",name,entryKeyId,e.getValue(),addValueDebug,addColumnDebug);
    			DBObject addEntry = MongoQueryBuilder.createNew(key,cb,e.getValue());
    			addEntry.put(MongoQueryBuilder.COLUMN_NAME_DEBUG, addColumnDebug);
    			addEntry.put(MongoQueryBuilder.VALUE_DEBUG, addColumnDebug);
    			builder.insert(addEntry);
            }
    	    BulkWriteResult result  = builder.execute();
    	    logger.trace("BulkWriteResult:{}",result);
        } //if !additions
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws StorageException {
    	logger.debug("getKeys KeyRange: {}",query.getKeyStart(),query.getKeyEnd());
    	DBObject queryObject = MongoQueryBuilder.createKeyRangeQuery(query);
        List<String> keys = dbCollection.distinct(MongoQueryBuilder.KEY,queryObject);
    	return new RowIterator(dbCollection,keys, query, txh);
    }

    
    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
    	logger.debug("getKeys sliceQuery");
    	DBObject queryObject = MongoQueryBuilder.createSliceQuery(query);
        List<String> keys = dbCollection.distinct(MongoQueryBuilder.KEY,queryObject);
    	return new RowIterator(dbCollection,keys, query, txh);
    }

    @Override
    public String getName() {
        return name;
    }

    public void clear() {
        kcv.clear();        
        WriteResult result = dbCollection.remove(MongoQueryBuilder.EMPTY);
        logger.trace("clear result:"+result);
    }
    
    @Override
    public void close() throws StorageException {
    	logger.trace("close()");
        kcv.clear();
    }

 
    private static class RowIterator implements KeyIterator {
        //private DBCursor dbCursor;
        private final SliceQuery columnSlice;
        private final StoreTransaction transaction;
        private List<String> keys = null; 
        private boolean isClosed;
        Iterator<String> rowKeyIterator = null;
        private String currentRowKey = null;
		private DBCollection dbCollection = null;
        
        public RowIterator(DBCollection dbCollection, List<String> keys, SliceQuery query,StoreTransaction transaction) {
        	this.keys = keys;
            this.columnSlice = query;
            this.transaction = transaction;   
            this.rowKeyIterator = keys.iterator();
            this.dbCollection  = dbCollection;
		}

		@Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            if (currentRowKey == null){
                throw new IllegalStateException("getEntries() requires currentRowKey to be set, need to call next() first.");
            }
            
        //    final KeySliceQuery keySlice = new KeySliceQuery(currentRow.getKey(), columnSlice);
            return new RecordIterator<Entry>() {
            	DBObject queryObject = MongoQueryBuilder.createKeySliceQuery(currentRowKey, columnSlice);
                private final Iterator<DBObject> items = dbCollection.find(queryObject).limit(columnSlice.getLimit()).iterator();
            	            	
                @Override
                public boolean hasNext() {
                    ensureOpen();
                    return items.hasNext();                    
                }

                @Override
                public Entry next() {
                   ensureOpen();
                   Entry entry = MongoQueryBuilder.convertMongoObjectToEntry(items.next());
                   return entry;                    
                }

                @Override
                public void close() {
                    isClosed = true;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Column removal not supported");
                }
            }; 
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return rowKeyIterator.hasNext();

        }

        @Override
        public StaticBuffer next() {
            ensureOpen();
            currentRowKey = rowKeyIterator.next();
            StaticBuffer next = MongoConversionUtil.mongoFieldNameHex2StaticBuffer(currentRowKey);          
            return next;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        private void ensureOpen() {
            if (isClosed){
                throw new IllegalStateException("Iterator has been closed.");
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Key removal not supported");
        }
    }// RowIterator

    

}
