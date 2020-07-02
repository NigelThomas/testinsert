package com.sqlstream.util;

import java.sql.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public class TestInserts
{
    public static final Logger tracer =
        Logger.getLogger(TestInserts.class.getName());

 
    @Option(
            name = "--help",
            usage = "print help message and quit",
            required = false)
    private boolean help = false;

    
    @Option(
            name = "-b",
            aliases = {"--batch-size"},
            usage = "insert <rows> rows in each batch, for each thread",
            metaVar = "batchSize",
            required = false)

    private int batchSize = 1000;

    @Option(
        name = "-c",
        aliases = {"--batch-count"},
        usage = "insert a total of <bathch-count> batches",
        metaVar = "batchCount",
        required = false)

    private int batchCount = 1000;

    @Option(
            name = "-t",
            aliases = {"--thread-count"},
            usage = "launch <thread-count> threads",
            metaVar = "threadCount",
            required = false)

    static private int threadCount = 2; 

    @Option(
            name = "-w",
            aliases = {"--wait-time"},
            usage = "wait an average of <wait-time> secs between batches - if 0 then don't wait",
            metaVar = "waitTime",
            required = false)

    static private long waitTime = 0; 

    
    @Option(
        name = "-s",
        aliases = {"--stream-name"},
        usage = "name of the stream in which to insert",
        metaVar = "streamname",
        required = false)
    static String streamName = "\"testinsert\".\"insertstream\"";



    /*
    @Option(
            name = "-su",
            aliases = {"--sqlstream-url"},
            usage = "jdbc URL to the sqlstream server",
            metaVar = "URL",
            required = false)
    private String sqlstreamUrl = "";

    @Option(
            name = "-sn",
            aliases = {"--sqlstream-name"},
            usage = "user name on the sqlstream server",
            metaVar = "NAME",
            required = false)
    private String sqlstreamName = "";

    @Option(
            name = "-sp",
            aliases = {"--sqlstream-password"},
            usage = "user password on the sqlstream server",
            metaVar = "PASSWORD",
            required = false)
    private String sqlstreamPassword = "";
    */

    private void usage(CmdLineParser parser) throws IOException
    {
        System.err.println(
                "testinsert.sh [OPTIONS...] ARGUMENTS...");
        parser.printUsage(System.err);
        System.err.println();
    }

    public void initialize(String[] args) throws IOException
    {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(120);
        try {
            parser.parseArgument(args);
            if (help) {
                usage(parser);
                System.exit(0);
            }
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            usage(parser);
            System.exit(-1);
        }

    }

    public static void main(String[] args)  {
        tracer.info("Starting");

        TestInserts ti = new TestInserts();


        ti.execute(args);
    } 

    void execute(String[] args) {
        int threadNo = -1;
        try {
            initialize(args);

            tracer.info("batchSize="+batchSize+", batchCount="+batchCount+
                        ", threadCount="+threadCount+", waitTime="+waitTime
                        );

            Thread[] threads = new Thread[threadCount];       

            for (threadNo = 0; threadNo < threads.length; threadNo++ ) {
                TestThread tt = new TestThread(threadNo, batchSize, batchCount, streamName);
                threads[threadNo] = new Thread(tt);
                threads[threadNo].start();
            }

            // wait for all threads to finish

            for (threadNo = 0; threadNo < threads.length; threadNo++ ) {
                try {
                    threads[threadNo].join();
                    tracer.info("test thread "+threadNo+" finished");
                } catch (InterruptedException ie) {
                    tracer.log(Level.SEVERE,"test thread "+threadNo+" interrupted",ie);
                }
            }

        } catch (IOException ioe) {
            tracer.log(Level.WARNING, "IOException getting arguments ", ioe);
        }
    } 

    static class TestThread implements Runnable {

        public static final Logger tracer =
             Logger.getLogger(TestThread.class.getName());
    
        int batchNo = 0;
        int threadNo =0;
        int batchSize = 0;
        int batchCount = 0;
        String streamName;

        Connection connection = null;

        public TestThread(int threadNo, int batchSize, int batchCount, String streamName) {

            this.threadNo = threadNo;
            this.batchSize = batchSize;
            this.batchCount = batchCount;
            this.streamName = streamName;

        }
    
        public void run() {

            int i = 0;
            
            String insertSQL = 
                "INSERT EXPEDITED INTO "+streamName+
                " (thread, batch, rec, d1, d2, vc1, vc2)"+
                " values (?,?, ?, ?, ?, ?, ?)";
            ;

            try {
                tracer.info("Thread "+threadNo+" has started");
                connection = DriverManager.getConnection("jdbc:sqlstream:sdp://localhost:5570;user=sa;password=mumble;autoCommit=false");
                tracer.finer("Thread "+threadNo+" has connected");
                PreparedStatement ps = connection.prepareStatement(insertSQL);
                tracer.finer("Thread "+threadNo+" has prepared a statement");

                for (batchNo = 0; batchNo < batchCount; batchNo++) {

                    for (i = 0; i < batchSize; i++) {
                        int c = 1;
                        ps.setInt(c++, threadNo);
                        ps.setInt(c++, batchNo);
                        ps.setInt(c++, i);
                        ps.setDouble(c++, (double) (batchNo * batchSize+i));
                        ps.setDouble(c++, (double) threadNo);
                        ps.setString(c++,"A string");
                        ps.setString(c++,"More text");

                        ps.executeUpdate();
                    } 
                    connection.commit();

                    // pick a random wait time somewhere between 1 and (waitTime+1) seconds unless waitTime is zero
                    if (waitTime > 0) {
                        long sleepTime = 1000 + waitTime * (long) (2000.0 * Math.random());
                        tracer.fine("Thread "+threadNo+" batch "+batchNo+" processed, sleeping for "+sleepTime+"ms");
                        Thread.sleep(sleepTime);
                        tracer.finer("Thread "+threadNo+" sleep finished");
                    }
                } 

                tracer.info("Thread "+threadNo+" finished");
            } catch (SQLException sqle) {
                tracer.log(Level.SEVERE, "SQL error in thread "+threadNo+" at batch "+batchNo+" row "+i, sqle);
                return;
            } catch (InterruptedException ie) {
                tracer.log(Level.WARNING, "interruptedException in thread "+threadNo+" at batch "+batchNo+" row "+i, ie);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException sqle2) {
                        tracer.log(Level.SEVERE,"Failure closing connection in thread "+threadNo, sqle2);
                    }
                }
            }

        }
    }
}
