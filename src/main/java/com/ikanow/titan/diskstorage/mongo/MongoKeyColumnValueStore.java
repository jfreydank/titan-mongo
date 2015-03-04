package com.ikanow.titan.diskstorage.mongo;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
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
import com.thinkaurelius.titan.diskstorage.util.StaticArrayEntry;

/**
 * A MongoDB implementation of {@link KeyColumnValueStore}.
 *
 * @author Joern Freydank jfreydank@ikanow.com
 */

public class MongoKeyColumnValueStore implements KeyColumnValueStore {

    private static final Logger logger = LoggerFactory.getLogger(MongoKeyColumnValueStore.class);

    private final String name;
    private final ConcurrentNavigableMap<StaticBuffer, ColumnValueStore> kcv;
    protected DB db = null;
    protected DBCollection dbCollection = null;
    
    public MongoKeyColumnValueStore(@Nonnull DB db,final String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name = name;
        this.db = db;
        this.dbCollection=db.getCollection(name);
        this.kcv = new ConcurrentSkipListMap<StaticBuffer, ColumnValueStore>();
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws StorageException {
    	logger.debug("getSlice S:{} K:{},start:{} end;{}",name,MongoConversionUtil.staticBuffer2Bytes(query.getKey()),MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceStart()),MongoConversionUtil.staticBuffer2MongoFieldNameHex(query.getSliceEnd()));
    	EntryList slice = null;
        ColumnValueStore cvs = kcv.get(query.getKey());
        if (cvs == null){
        	slice = EntryList.EMPTY_LIST;
        }
        else{
        	slice = cvs.getSlice(query, txh);
        }
        
        //mongo
        // TODO replace findONe with queryOBject
        DBObject mongoEntry = dbCollection.findOne(); 
        EntryList slice2 = convertMongoObjectToEntryList(mongoEntry,query.getSliceStart(),query.getSliceEnd());  
        logger.debug("getSliceReturn: {} vs {}",slice,slice2);
        return slice2;
    }

    
    protected EntryList convertMongoObjectToEntryList(DBObject mongoEntry, StaticBuffer sliceStart, StaticBuffer sliceEnd) {
    	EntryList entryList = null;
    	if((mongoEntry==null) || (sliceStart==null && sliceEnd==null)){
    		entryList = EntryList.EMPTY_LIST;
    	}else{    		
    		TreeSet<String> fieldNames = new TreeSet<String>(mongoEntry.keySet());    		
    		// remove MongoDB specific field names, _ID and  key
    		fieldNames.removeAll(MongoQueryBuilder.RESERVED_FIELDNAMES);
    		
    		// snip out fieldNames that are in range
    		String sliceStartName = MongoConversionUtil.staticBuffer2MongoFieldNameHex(sliceStart);
    		String sliceEndName = MongoConversionUtil.staticBuffer2MongoFieldNameHex(sliceEnd);
    		SortedSet<String> fieldNamesSlice = fieldNames.subSet(sliceStartName,true, sliceEndName,true);
    		entryList = new MemoryEntryList(fieldNamesSlice.size());
    		for (String fieldName : fieldNamesSlice) {
    			StaticBuffer sbColumn = MongoConversionUtil.mongoFieldNameHex2StaticBuffer(fieldName);
    			byte[] columnValue = (byte[])mongoEntry.get(fieldName);
    			StaticBuffer sbValue = MongoConversionUtil.mongoFieldValue2StaticBuffer(columnValue);
    			logger.debug("convertMongoObjectToEntryList Converted StaticBuffer for {}={}",columnValue,sbValue);
				Entry e = StaticArrayEntry.of(sbColumn, sbValue);
				entryList.add(e);
			}
    	}
		return entryList;
	}

 
    @Override
    public Map<StaticBuffer,EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws StorageException {
        Map<StaticBuffer,EntryList> result = Maps.newHashMap();
        for (StaticBuffer key : keys) result.put(key,getSlice(new KeySliceQuery(key,query),txh));
        return result;
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws StorageException {
    	
    	byte[] entryKey = MongoConversionUtil.staticBuffer2Bytes(key); 
        logger.debug("mutate:S:{} E:{}",name,entryKey);
        
    	ColumnValueStore cvs = kcv.get(key);
        
        if (cvs == null) {
            kcv.putIfAbsent(key, new ColumnValueStore());
            cvs = kcv.get(key);
        }
        cvs.mutate(additions, deletions, txh);
       
        // mongo
        DBObject emptyWithKey = MongoQueryBuilder.createEmptyWithKey(key);
        DBObject entry = dbCollection.findOne();
        if(entry!=null){
	        for (StaticBuffer deletion : deletions) {
	            String delColumn = MongoConversionUtil.staticBuffer2MongoFieldNameHex(deletion);
	        	Object removedValue = entry.removeField(delColumn);
	            logger.debug("mutate:S:{} E:{} D:{} V{}:",name,entryKey,delColumn,removedValue);
	        }
        }else{
        	// reuse query template objects
        	entry = emptyWithKey;
        }
        if (!additions.isEmpty()) {
            for (Entry e : additions) {      
            	StaticBuffer cb = e.getColumn(); 
            	String addColumn = MongoConversionUtil.staticBuffer2MongoFieldNameHex(cb);
            	String addColumnDebug = MongoConversionUtil.staticBuffer2String(cb);

            	byte[] addValue = MongoConversionUtil.staticBuffer2Bytes(e.getValue());
    			logger.debug("mutate:S:{} E:{} Converted StaticBuffer1 for {}={}",name,entryKey,e.getValue(),addValue);
//    			StaticBuffer sbValue = MongoConversionUtil.mongoFieldValue2StaticBuffer(addValue);
//    			logger.debug("Mutate Converted StaticBuffer2 for {}={}",addValue,sbValue);
	        	entry.put(addColumn,addValue);
	        	logger.debug("mutate:S:{} E:{} A:{} V{}:",name,entryKey,cb,addValue,addColumnDebug);
            }
        } //if !additions
        dbCollection.save(entry);
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(final KeyRangeQuery query, final StoreTransaction txh) throws StorageException {
    	logger.debug("getKeys KeyRange: {}",query.getKeyStart(),query.getKeyEnd());
    	DBObject queryObject = MongoQueryBuilder.createKeyQuery(query);
        DBCursor dbCursor = dbCollection.find(queryObject);
    	return new RowIterator(dbCursor, query, txh);
    }

    
    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws StorageException {
    	logger.debug("getKeys sliceQuery");
    	DBObject queryObject = MongoQueryBuilder.createSliceQuery(query);
        DBCursor dbCursor = dbCollection.find(queryObject);
    	return new RowIterator(dbCursor, query, txh);
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
        private DBCursor dbCursor;
        private final SliceQuery columnSlice;
        private final StoreTransaction transaction;

        private boolean isClosed;


        public RowIterator(DBCursor dbCursor, SliceQuery query,StoreTransaction transaction) {
        	this.dbCursor= dbCursor;
            this.columnSlice = query;
            this.transaction = transaction;        	
		}

		@Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();

            if (columnSlice == null){
                throw new IllegalStateException("getEntries() requires SliceQuery to be set.");
            }
            
        //    final KeySliceQuery keySlice = new KeySliceQuery(currentRow.getKey(), columnSlice);
            return new RecordIterator<Entry>() {
               // private final Iterator<Entry> items = currentRow.getValue().getSlice(keySlice, transaction).iterator();
            	DBObject mongoEntry = null;
            	
                @Override
                public boolean hasNext() {
                    ensureOpen();
                    //return items.hasNext();
                    return false;
                }

                @Override
                public Entry next() {
                    ensureOpen();
                    //return items.next();
                    return null;
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
            boolean retVal = false;
            /*
            if (null != nextRow)
                return true;

            while (rows.hasNext()) {
                nextRow = rows.next();
                List<Entry> ents = nextRow.getValue().getSlice(new KeySliceQuery(nextRow.getKey(), columnSlice), transaction);
                if (null != ents && 0 < ents.size())
                    break;
            }

            return null != nextRow;
            */
            return retVal;
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();
            StaticBuffer next = null;
            /*
            Preconditions.checkNotNull(nextRow);

            currentRow = nextRow;
            nextRow = null;
            ;

            return currentRow.getKey();
            */
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
