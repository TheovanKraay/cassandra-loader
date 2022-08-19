package com.azure.cassandrami.examples;

import com.azure.cassandrami.repository.UserRepository;
import com.azure.cassandrami.util.Configurations;
import com.datastax.oss.driver.api.core.CqlSession;
import com.github.javafaker.Faker;

import java.io.IOException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load data into Cassandra
 */
public class UserProfile {

    public int NUMBER_OF_THREADS = 1;
    public int NUMBER_OF_WRITES_PER_THREAD = 2;

    Queue<String> docIDs = new ConcurrentLinkedQueue<String>();
    AtomicInteger exceptionCount = new AtomicInteger(0);
    AtomicLong insertCount = new AtomicLong(0);
    AtomicInteger recordCount = new AtomicInteger(0);
    AtomicInteger verifyCount = new AtomicInteger(0);
    AtomicLong totalLatency = new AtomicLong(0);
    AtomicLong totalReadLatency = new AtomicLong(0);
    private static Configurations config = new Configurations();

    public void loadTest(final String keyspace, final String table, final UserRepository repository,
            final UserProfile u, final String preparedStatement, final String finalQuery, final int noOfThreads,
            final int noOfWritesPerThread) throws InterruptedException, NumberFormatException, IOException {

        final Faker faker = new Faker();
        final ExecutorService es = Executors.newCachedThreadPool();
        int timeout = Integer.parseInt(config.getProperty("loadTimeout"));
        System.out.println("Loading data (will timeout after "+timeout+" minutes)....");
        for (int i = 1; i <= noOfThreads; i++) {
            final Runnable task = () -> {
                for (int j = 1; j <= noOfWritesPerThread; j++) {
                    final UUID guid = java.util.UUID.randomUUID();
                    final String strGuid = guid.toString();
                    this.docIDs.add(strGuid);
                    try {
                        final String name = faker.name().lastName();
                        final String city = faker.address().city();
                        u.recordCount.incrementAndGet();
                        final long startTime = System.currentTimeMillis();
                        repository.insertUser(preparedStatement, guid.toString(), name, city);
                        final long endTime = System.currentTimeMillis();
                        final long duration = (endTime - startTime);
                        //System.out.println("insert duration time millis: " + duration);
                        this.totalLatency.getAndAdd(duration);
                        u.insertCount.incrementAndGet();
                    } catch (final Exception e) {
                        u.exceptionCount.incrementAndGet();
                        System.out.println("Exception: " + e);
                    }
                }
            };
            es.execute(task);
        }
        es.shutdown();
        
        final boolean finished = es.awaitTermination(timeout, TimeUnit.MINUTES);
        if (finished) {
            final long latency = (this.totalLatency.get() / this.insertCount.get());
            System.out.println("number of records loaded: "+this.insertCount.get());
            System.out.println("Finished executing all threads.");
            System.out.print("Average write Latency in milliseconds: " + latency + "\n");
            Thread.sleep(1000);
        }
    }

    public static void main(final String[] s) throws Exception {

        final UserProfile u = new UserProfile();
        final String keyspace = "uprofile";
        final String table = "user";
        String DC = config.getProperty("DC");
        System.out.println("Creating Cassandra session...");
        CqlSession cassandraSource = CqlSession.builder().withLocalDatacenter(DC).build();
        int NUMBER_OF_WRITES_PER_THREAD = Integer.parseInt(config.getProperty("threads"));
        int NUMBER_OF_THREADS = Integer.parseInt(config.getProperty("records"));        
        final UserRepository sourcerepository = new UserRepository(cassandraSource);

        try {

            // Create keyspace and table in cassandra cource database
            System.out.println("Dropping source keyspace " + keyspace + " (if exists)... ");
            sourcerepository.deleteTable("DROP KEYSPACE IF EXISTS " + keyspace + "");
            System.out.println("Done dropping source keyspace " + keyspace + ".");
            System.out.println("Creating keyspace: " + keyspace + "... ");
            sourcerepository.createKeyspace("CREATE KEYSPACE " + keyspace
                    + " WITH REPLICATION = {'class':'NetworkTopologyStrategy', '"+DC+"' :3}");
            System.out.println("Done creating keyspace: " + keyspace + ".");
            System.out.println("Creating table: " + table + "...");            
            sourcerepository.createTable("CREATE TABLE " + keyspace + "." + table
                    + " (user_id text PRIMARY KEY, user_name text, user_bcity text)");
            System.out.println("Done creating table: " + table + "."); 
            Thread.sleep(1000);


            // Setup load test queries
            final String loadTestPreparedStatement = "insert into " + keyspace + "." + table + " (user_bcity,user_id,"
                    + "user_name) VALUES (?,?,?)";
            final String loadTestFinalSelectQuery = "SELECT COUNT(*) as coun FROM " + keyspace + "." + table + "";

            // Run Load Test - Insert rows into user table
            u.loadTest(keyspace, table, sourcerepository, u, loadTestPreparedStatement, loadTestFinalSelectQuery,
                    NUMBER_OF_THREADS, NUMBER_OF_WRITES_PER_THREAD);
        } catch (final Exception e) {
            System.out.println("Main Exception " + e);
        }
        System.out.println("Finished loading data.");
        System.exit(0);
    }
}
