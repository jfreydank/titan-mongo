package com.ikanow.titan.diskstorage.mongo;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.blueprints.TitanBlueprintsTest;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphTestSuite;
import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.VertexTestSuite;

/**
 * @author Joern Freydank
 */

public class MongoBlueprintsTest extends TitanBlueprintsTest {

    private static final String DEFAULT_SUBDIR = "standard";

    private static final Logger logger =
            LoggerFactory.getLogger(MongoBlueprintsTest.class);

    @Override
    public Graph generateGraph() {
        return generateGraph(DEFAULT_SUBDIR);
    }

  //  private final Map<String, TitanGraph> openGraphs = new HashMap<String, TitanGraph>();

    @Override
    public Graph generateGraph(String uid) {
                try {
                    MongoStoreManager s = new MongoStoreManager(MongoStorageSetup.getMongoConfiguration());
                    s.clearStorage();
                    s.close();
                    //File dirFile = new File(dir);
                    //Assert.assertFalse(dirFile.exists() && dirFile.listFiles().length > 0);
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                }
                Graph g = TitanFactory.open(MongoStorageSetup.getMongoConfiguration());
        return g;
    }

    @Override
    public boolean supportsMultipleGraphs() {
        return false;
    }

    @Override
    public void cleanUp() throws StorageException {
    }


    @Override
    public void beforeSuite() {
        //Nothing
    }


    @Override
    public void afterSuite() {
    }
    
    public void testVertexSelected() throws Exception {
        //this.stopWatch();
        
        Set<String> ignoreTests = new HashSet<String>();
        ignoreTests.add("testAddingIdProperty");
        ignoreTests.add("testAddManyVertexProperties");
        ignoreTests.add("testAddVertexProperties");
//        ignoreTests.add("testBasicAddVertex");
        ignoreTests.add("testConcurrentModificationOnProperties");
        ignoreTests.add("testEmptyKeyProperty");
        ignoreTests.add("testGetNonExistantVertices");
        ignoreTests.add("testGettingEdgesAndVertices");
        ignoreTests.add("testGetVertexWithNull");
        ignoreTests.add("testLegalVertexEdgeIterables");
        ignoreTests.add("testNoConcurrentModificationException");
        ignoreTests.add("testRemoveVertex");
        ignoreTests.add("testRemoveVertexNullId");
        ignoreTests.add("testRemoveVertexProperties");
        ignoreTests.add("testRemoveVertexWithEdges");
        ignoreTests.add("testSettingBadVertexProperties");
        ignoreTests.add("testVertexCentricLinking");
        ignoreTests.add("testVertexCentricRemoving");
        ignoreTests.add("testVertexEquality");
        ignoreTests.add("testVertexEqualityForSuppliedIdsAndHashCode");
        ignoreTests.add("testVertexIterator");
     	
        doTestSuite(new VertexTestSuite(this),ignoreTests);
     //   printTestPerformance("testVertexSelected", this.stopWatch());
    }

    public void testGraphTestSuite() throws Exception {
       this.stopWatch();                       //Excluded test case because toString representation is non-standard
        Set<String> ignoreTests = new HashSet<String>();
        ignoreTests.add("testStringRepresentation");
        doTestSuite(new GraphTestSuite(this), ignoreTests);
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }


}
