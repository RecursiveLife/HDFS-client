/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.ArrayUtils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem.Statistics.StatisticsData;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.MD5MD5CRC32FileChecksum;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.StorageStatistics.LongStatistic;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.VolumeId;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.LeaseRenewer;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.server.namenode.top.window.RollingWindowManager.Op;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDistributedFileSystem {
  private static final Random RAN = new Random();
  private static final Logger LOG = LoggerFactory.getLogger(
      TestDistributedFileSystem.class);

  static {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }

  private boolean dualPortTesting = false;
  
  private boolean noXmlDefaults = false;
  
  private HdfsConfiguration getTestConfiguration() {
    HdfsConfiguration conf;
    if (noXmlDefaults) {
      conf = new HdfsConfiguration(false);
      String namenodeDir = new File(MiniDFSCluster.getBaseDirectory(), "name").
          getAbsolutePath();
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, namenodeDir);
      conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, namenodeDir);
    } else {
      conf = new HdfsConfiguration();
    }
    if (dualPortTesting) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
          "localhost:0");
    }
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);

    return conf;
  }

  @Test
  public void testEmptyDelegationToken() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fileSys = cluster.getFileSystem();
      fileSys.getDelegationToken("");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testFileSystemCloseAll() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).
        build();
    URI address = FileSystem.getDefaultUri(conf);

    try {
      FileSystem.closeAll();

      conf = getTestConfiguration();
      FileSystem.setDefaultUri(conf, address);
      FileSystem.get(conf);
      FileSystem.get(conf);
      FileSystem.closeAll();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Tests DFSClient.close throws no ConcurrentModificationException if 
   * multiple files are open.
   * Also tests that any cached sockets are closed. (HDFS-3359)
   */
  @Test
  public void testDFSClose() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem fileSys = cluster.getFileSystem();
      
      // create two files, leaving them open
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.create(new Path("/test/dfsclose/file-1"));
      
      // create another file, close it, and read it, so
      // the client gets a socket in its SocketCache
      Path p = new Path("/non-empty-file");
      DFSTestUtil.createFile(fileSys, p, 1L, (short)1, 0L);
      DFSTestUtil.readFile(fileSys, p);
      
      fileSys.close();
      
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testDFSCloseOrdering() throws Exception {
    DistributedFileSystem fs = new MyDistributedFileSystem();
    Path path = new Path("/a");
    fs.deleteOnExit(path);
    fs.close();

    InOrder inOrder = inOrder(fs.dfs);
    inOrder.verify(fs.dfs).closeOutputStreams(eq(false));
    inOrder.verify(fs.dfs).delete(eq(path.toString()), eq(true));
    inOrder.verify(fs.dfs).close();
  }
  
  private static class MyDistributedFileSystem extends DistributedFileSystem {
    MyDistributedFileSystem() {
      dfs = mock(DFSClient.class);
    }
    @Override
    public boolean exists(Path p) {
      return true; // trick out deleteOnExit
    }
    // Symlink resolution doesn't work with a mock, since it doesn't
    // have a valid Configuration to resolve paths to the right FileSystem.
    // Just call the DFSClient directly to register the delete
    @Override
    public boolean delete(Path f, final boolean recursive) throws IOException {
      return dfs.delete(f.toUri().getPath(), recursive);
    }
  }

  @Test
  public void testDFSSeekExceptions() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      FileSystem fileSys = cluster.getFileSystem();
      String file = "/test/fileclosethenseek/file-0";
      Path path = new Path(file);
      // create file
      FSDataOutputStream output = fileSys.create(path);
      output.writeBytes("Some test data to write longer than 10 bytes");
      output.close();
      FSDataInputStream input = fileSys.open(path);
      input.seek(10);
      boolean threw = false;
      try {
        input.seek(100);
      } catch (IOException e) {
        // success
        threw = true;
      }
      assertTrue("Failed to throw IOE when seeking past end", threw);
      input.close();
      threw = false;
      try {
        input.seek(1);
      } catch (IOException e) {
        //success
        threw = true;
      }
      assertTrue("Failed to throw IOE when seeking after close", threw);
      fileSys.close();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testDFSClient() throws Exception {
    Configuration conf = getTestConfiguration();
    final long grace = 1000L;
    MiniDFSCluster cluster = null;
    LeaseRenewer.setLeaseRenewerGraceDefault(grace);

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      final String filepathstring = "/test/LeaseChecker/foo";
      final Path[] filepaths = new Path[4];
      for(int i = 0; i < filepaths.length; i++) {
        filepaths[i] = new Path(filepathstring + i);
      }
      final long millis = Time.now();

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Method checkMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("isRunning");
        checkMethod.setAccessible(true);
        assertFalse((boolean) checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
  
        {
          //create a file
          final FSDataOutputStream out = dfs.create(filepaths[0]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //write something
          out.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close
          out.close();
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          for(int i = 0; i < 3; i++) {
            if ((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer())) {
              Thread.sleep(grace/2);
            }
          }
          //passed grace period
          assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        {
          //create file1
          final FSDataOutputStream out1 = dfs.create(filepaths[1]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //create file2
          final FSDataOutputStream out2 = dfs.create(filepaths[2]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

          //write something to file1
          out1.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file1
          out1.close();
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

          //write something to file2
          out2.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file2
          out2.close();
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        {
          //create file3
          final FSDataOutputStream out3 = dfs.create(filepaths[3]);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          Thread.sleep(grace/4*3);
          //passed previous grace period, should still running
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //write something to file3
          out3.writeLong(millis);
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          //close file3
          out3.close();
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
          for(int i = 0; i < 3; i++) {
            if ((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer())) {
              Thread.sleep(grace/2);
            }
          }
          //passed grace period
          assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        }

        dfs.close();
      }

      {
        // Check to see if opening a non-existent file triggers a FNF
        FileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/wrwelkj");
        assertFalse("File should not exist for test.", fs.exists(dir));

        try {
          FSDataInputStream in = fs.open(dir);
          try {
            in.close();
            fs.close();
          } finally {
            assertTrue("Did not get a FileNotFoundException for non-existing" +
                " file.", false);
          }
        } catch (FileNotFoundException fnf) {
          // This is the proper exception to catch; move on.
        }

      }

      {
        final DistributedFileSystem dfs = cluster.getFileSystem();
        Method checkMethod = dfs.dfs.getLeaseRenewer().getClass()
            .getDeclaredMethod("isRunning");
        checkMethod.setAccessible(true);
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));

        //open and check the file
        FSDataInputStream in = dfs.open(filepaths[0]);
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        assertEquals(millis, in.readLong());
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        in.close();
        assertFalse((boolean)checkMethod.invoke(dfs.dfs.getLeaseRenewer()));
        dfs.close();
      }
      
      { // test accessing DFS with ip address. should work with any hostname
        // alias or ip address that points to the interface that NameNode
        // is listening on. In this case, it is localhost.
        String uri = "hdfs://127.0.0.1:" + cluster.getNameNodePort() + 
                      "/test/ipAddress/file";
        Path path = new Path(uri);
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FSDataOutputStream out = fs.create(path);
        byte[] buf = new byte[1024];
        out.write(buf);
        out.close();
        
        FSDataInputStream in = fs.open(path);
        in.readFully(buf);
        in.close();
        fs.close();
      }
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  /**
   * This is to test that the {@link FileSystem#clearStatistics()} resets all
   * the global storage statistics.
   */
  @Test
  public void testClearStatistics() throws Exception {
    final Configuration conf = getTestConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      FileSystem dfs = cluster.getFileSystem();

      final Path dir = new Path("/testClearStatistics");
      final long mkdirCount = getOpStatistics(OpType.MKDIRS);
      long writeCount = DFSTestUtil.getStatistics(dfs).getWriteOps();
      dfs.mkdirs(dir);
      checkOpStatistics(OpType.MKDIRS, mkdirCount + 1);
      assertEquals(++writeCount,
          DFSTestUtil.getStatistics(dfs).getWriteOps());

      final long createCount = getOpStatistics(OpType.CREATE);
      FSDataOutputStream out = dfs.create(new Path(dir, "tmpFile"), (short)1);
      out.write(40);
      out.close();
      checkOpStatistics(OpType.CREATE, createCount + 1);
      assertEquals(++writeCount,
          DFSTestUtil.getStatistics(dfs).getWriteOps());

      FileSystem.clearStatistics();
      checkOpStatistics(OpType.MKDIRS, 0);
      checkOpStatistics(OpType.CREATE, 0);
      checkStatistics(dfs, 0, 0, 0);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testStatistics() throws IOException {
    FileSystem.getStatistics(HdfsConstants.HDFS_URI_SCHEME,
        DistributedFileSystem.class).reset();
    @SuppressWarnings("unchecked")
    ThreadLocal<StatisticsData> data = (ThreadLocal<StatisticsData>)
        Whitebox.getInternalState(
        FileSystem.getStatistics(HdfsConstants.HDFS_URI_SCHEME,
        DistributedFileSystem.class), "threadData");
    data.set(null);

    int lsLimit = 2;
    final Configuration conf = getTestConfiguration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, lsLimit);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      final FileSystem fs = cluster.getFileSystem();
      Path dir = new Path("/test");
      Path file = new Path(dir, "file");

      int readOps = 0;
      int writeOps = 0;
      int largeReadOps = 0;

      long opCount = getOpStatistics(OpType.MKDIRS);
      fs.mkdirs(dir);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.MKDIRS, opCount + 1);
      
      opCount = getOpStatistics(OpType.CREATE);
      FSDataOutputStream out = fs.create(file, (short)1);
      out.close();
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.CREATE, opCount + 1);

      opCount = getOpStatistics(OpType.GET_FILE_STATUS);
      FileStatus status = fs.getFileStatus(file);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_FILE_STATUS, opCount + 1);
      
      opCount = getOpStatistics(OpType.GET_FILE_BLOCK_LOCATIONS);
      fs.getFileBlockLocations(file, 0, 0);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_FILE_BLOCK_LOCATIONS, opCount + 1);
      fs.getFileBlockLocations(status, 0, 0);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_FILE_BLOCK_LOCATIONS, opCount + 2);
      
      opCount = getOpStatistics(OpType.OPEN);
      FSDataInputStream in = fs.open(file);
      in.close();
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.OPEN, opCount + 1);
      
      opCount = getOpStatistics(OpType.SET_REPLICATION);
      fs.setReplication(file, (short)2);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.SET_REPLICATION, opCount + 1);
      
      opCount = getOpStatistics(OpType.RENAME);
      Path file1 = new Path(dir, "file1");
      fs.rename(file, file1);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.RENAME, opCount + 1);
      
      opCount = getOpStatistics(OpType.GET_CONTENT_SUMMARY);
      fs.getContentSummary(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_CONTENT_SUMMARY, opCount + 1);
      
      
      // Iterative ls test
      long mkdirOp = getOpStatistics(OpType.MKDIRS);
      long listStatusOp = getOpStatistics(OpType.LIST_STATUS);
      for (int i = 0; i < 10; i++) {
        Path p = new Path(dir, Integer.toString(i));
        fs.mkdirs(p);
        mkdirOp++;
        FileStatus[] list = fs.listStatus(dir);
        if (list.length > lsLimit) {
          // if large directory, then count readOps and largeReadOps by 
          // number times listStatus iterates
          int iterations = (int)Math.ceil((double)list.length/lsLimit);
          largeReadOps += iterations;
          readOps += iterations;
          listStatusOp += iterations;
        } else {
          // Single iteration in listStatus - no large read operation done
          readOps++;
          listStatusOp++;
        }
        
        // writeOps incremented by 1 for mkdirs
        // readOps and largeReadOps incremented by 1 or more
        checkStatistics(fs, readOps, ++writeOps, largeReadOps);
        checkOpStatistics(OpType.MKDIRS, mkdirOp);
        checkOpStatistics(OpType.LIST_STATUS, listStatusOp);
      }
      
      opCount = getOpStatistics(OpType.GET_STATUS);
      fs.getStatus(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_STATUS, opCount + 1);

      opCount = getOpStatistics(OpType.GET_FILE_CHECKSUM);
      fs.getFileChecksum(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      checkOpStatistics(OpType.GET_FILE_CHECKSUM, opCount + 1);
      
      opCount = getOpStatistics(OpType.SET_PERMISSION);
      fs.setPermission(file1, new FsPermission((short)0777));
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.SET_PERMISSION, opCount + 1);
      
      opCount = getOpStatistics(OpType.SET_TIMES);
      fs.setTimes(file1, 0L, 0L);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.SET_TIMES, opCount + 1);

      opCount = getOpStatistics(OpType.SET_OWNER);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      fs.setOwner(file1, ugi.getUserName(), ugi.getGroupNames()[0]);
      checkOpStatistics(OpType.SET_OWNER, opCount + 1);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);

      opCount = getOpStatistics(OpType.DELETE);
      fs.delete(dir, true);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      checkOpStatistics(OpType.DELETE, opCount + 1);
      
    } finally {
      if (cluster != null) cluster.shutdown();
    }
    
  }

  @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
  @Test (timeout = 180000)
  public void testConcurrentStatistics()
      throws IOException, InterruptedException {
    FileSystem.getStatistics(HdfsConstants.HDFS_URI_SCHEME,
        DistributedFileSystem.class).reset();

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(
        new Configuration()).build();
    cluster.waitActive();
    final FileSystem fs = cluster.getFileSystem();
    final int numThreads = 5;
    final ExecutorService threadPool =
        HadoopExecutors.newFixedThreadPool(numThreads);

    try {
      final CountDownLatch allExecutorThreadsReady =
          new CountDownLatch(numThreads);
      final CountDownLatch startBlocker = new CountDownLatch(1);
      final CountDownLatch allDone = new CountDownLatch(numThreads);
      final AtomicReference<Throwable> childError = new AtomicReference<>();

      for (int i = 0; i < numThreads; i++) {
        threadPool.submit(new Runnable() {
          @Override
          public void run() {
            allExecutorThreadsReady.countDown();
            try {
              startBlocker.await();
              final FileSystem fs = cluster.getFileSystem();
              fs.mkdirs(new Path("/testStatisticsParallelChild"));
            } catch (Throwable t) {
              LOG.error("Child failed when calling mkdir", t);
              childError.compareAndSet(null, t);
            } finally {
              allDone.countDown();
            }
          }
        });
      }

      final long oldMkdirOpCount = getOpStatistics(OpType.MKDIRS);

      // wait until all threads are ready
      allExecutorThreadsReady.await();
      // all threads start making directories
      startBlocker.countDown();
      // wait until all threads are done
      allDone.await();

     assertNull("Child failed with exception " + childError.get(),
          childError.get());

      checkStatistics(fs, 0, numThreads, 0);
      // check the single operation count stat
      checkOpStatistics(OpType.MKDIRS, numThreads + oldMkdirOpCount);
      // iterate all the operation counts
      for (Iterator<LongStatistic> opCountIter =
           FileSystem.getGlobalStorageStatistics()
               .get(DFSOpsCountStatistics.NAME).getLongStatistics();
           opCountIter.hasNext();) {
        final LongStatistic opCount = opCountIter.next();
        if (OpType.MKDIRS.getSymbol().equals(opCount.getName())) {
          assertEquals("Unexpected op count from iterator!",
              numThreads + oldMkdirOpCount, opCount.getValue());
        }
        LOG.info(opCount.getName() + "\t" + opCount.getValue());
      }
    } finally {
      threadPool.shutdownNow();
      cluster.shutdown();
    }
  }

  /** Checks statistics. -1 indicates do not check for the operations */
  private void checkStatistics(FileSystem fs, int readOps, int writeOps,
      int largeReadOps) {
    assertEquals(readOps, DFSTestUtil.getStatistics(fs).getReadOps());
    assertEquals(writeOps, DFSTestUtil.getStatistics(fs).getWriteOps());
    assertEquals(largeReadOps, DFSTestUtil.getStatistics(fs).getLargeReadOps());
  }

  private static void checkOpStatistics(OpType op, long count) {
    assertEquals("Op " + op.getSymbol() + " has unexpected count!",
        count, getOpStatistics(op));
  }

  private static long getOpStatistics(OpType op) {
    return GlobalStorageStatistics.INSTANCE.get(
        DFSOpsCountStatistics.NAME)
        .getLong(op.getSymbol());
  }

  @Test
  public void testFileChecksum() throws Exception {
    GenericTestUtils.setLogLevel(HftpFileSystem.LOG, Level.ALL);

    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    final Configuration conf = getTestConfiguration();
    conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
    final FileSystem hdfs = cluster.getFileSystem();

    final String nnAddr = conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        current.getShortUserName() + "x", new String[]{"user"});
    
    try {
      hdfs.getFileChecksum(new Path(
          "/test/TestNonExistingFile"));
      fail("Expecting FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue("Not throwing the intended exception message", e.getMessage()
          .contains("File does not exist: /test/TestNonExistingFile"));
    }

    try {
      Path path = new Path("/test/TestExistingDir/");
      hdfs.mkdirs(path);
      hdfs.getFileChecksum(path);
      fail("Expecting FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue("Not throwing the intended exception message", e.getMessage()
          .contains("Path is not a file: /test/TestExistingDir"));
    }
    
    //hftp
    final String hftpuri = "hftp://" + nnAddr;
    System.out.println("hftpuri=" + hftpuri);
    final FileSystem hftp = ugi.doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return new Path(hftpuri).getFileSystem(conf);
      }
    });

    //webhdfs
    final String webhdfsuri = WebHdfsConstants.WEBHDFS_SCHEME + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = ugi.doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return new Path(webhdfsuri).getFileSystem(conf);
      }
    });

    final Path dir = new Path("/filechecksum");
    final int block_size = 1024;
    final int buffer_size = conf.getInt(
        CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096);
    conf.setInt(HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);

    //try different number of blocks
    for(int n = 0; n < 5; n++) {
      //generate random data
      final byte[] data = new byte[RAN.nextInt(block_size/2-1)+n*block_size+1];
      RAN.nextBytes(data);
      System.out.println("data.length=" + data.length);
  
      //write data to a file
      final Path foo = new Path(dir, "foo" + n);
      {
        final FSDataOutputStream out = hdfs.create(foo, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }
      
      //compute checksum
      final FileChecksum hdfsfoocs = hdfs.getFileChecksum(foo);
      System.out.println("hdfsfoocs=" + hdfsfoocs);

      //hftp
      final FileChecksum hftpfoocs = hftp.getFileChecksum(foo);
      System.out.println("hftpfoocs=" + hftpfoocs);

      final Path qualified = new Path(hftpuri + dir, "foo" + n);
      final FileChecksum qfoocs = hftp.getFileChecksum(qualified);
      System.out.println("qfoocs=" + qfoocs);

      //webhdfs
      final FileChecksum webhdfsfoocs = webhdfs.getFileChecksum(foo);
      System.out.println("webhdfsfoocs=" + webhdfsfoocs);

      final Path webhdfsqualified = new Path(webhdfsuri + dir, "foo" + n);
      final FileChecksum webhdfs_qfoocs =
          webhdfs.getFileChecksum(webhdfsqualified);
      System.out.println("webhdfs_qfoocs=" + webhdfs_qfoocs);

      //create a zero byte file
      final Path zeroByteFile = new Path(dir, "zeroByteFile" + n);
      {
        final FSDataOutputStream out = hdfs.create(zeroByteFile, false,
            buffer_size, (short)2, block_size);
        out.close();
      }

      //write another file
      final Path bar = new Path(dir, "bar" + n);
      {
        final FSDataOutputStream out = hdfs.create(bar, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }

      {
        // verify the magic val for zero byte file
        final FileChecksum zeroChecksum = hdfs.getFileChecksum(zeroByteFile);
        final String magicValue =
            "MD5-of-0MD5-of-0CRC32:70bc8f4b72a86921468bf8e8441dce51";
        // verify the magic val for zero byte files
        assertEquals(magicValue, zeroChecksum.toString());

        // verify checksum for empty file and 0 request length
        final FileChecksum checksumWith0 = hdfs.getFileChecksum(bar, 0);
        assertEquals(zeroChecksum, checksumWith0);

        // verify none existent file
        try {
          hdfs.getFileChecksum(new Path(dir, "none-existent"), 8);
          fail();
        } catch (Exception ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }

        // verify none existent file and 0 request length
        try {
          final FileChecksum noneExistentChecksumWith0 =
              hdfs.getFileChecksum(new Path(dir, "none-existent"), 0);
          fail();
        } catch (Exception ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }

        // verify checksums
        final FileChecksum barcs = hdfs.getFileChecksum(bar);
        final int barhashcode = barcs.hashCode();
        assertEquals(hdfsfoocs.hashCode(), barhashcode);
        assertEquals(hdfsfoocs, barcs);

        //hftp
        assertEquals(hftpfoocs.hashCode(), barhashcode);
        assertEquals(hftpfoocs, barcs);

        assertEquals(qfoocs.hashCode(), barhashcode);
        assertEquals(qfoocs, barcs);

        //webhdfs
        assertEquals(webhdfsfoocs.hashCode(), barhashcode);
        assertEquals(webhdfsfoocs, barcs);

        assertEquals(webhdfs_qfoocs.hashCode(), barhashcode);
        assertEquals(webhdfs_qfoocs, barcs);
      }

      hdfs.setPermission(dir, new FsPermission((short)0));
      { //test permission error on hftp 
        try {
          hftp.getFileChecksum(qualified);
          fail();
        } catch(IOException ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }
      }

      { //test permission error on webhdfs 
        try {
          webhdfs.getFileChecksum(webhdfsqualified);
          fail();
        } catch(IOException ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }
      }
      hdfs.setPermission(dir, new FsPermission((short)0777));
    }
    cluster.shutdown();
  }
  
  @Test
  public void testAllWithDualPort() throws Exception {
    dualPortTesting = true;

    try {
      testFileSystemCloseAll();
      testDFSClose();
      testDFSClient();
      testFileChecksum();
    } finally {
      dualPortTesting = false;
    }
  }
  
  @Test
  public void testAllWithNoXmlDefaults() throws Exception {
    // Do all the tests with a configuration that ignores the defaults in
    // the XML files.
    noXmlDefaults = true;

    try {
      testFileSystemCloseAll();
      testDFSClose();
      testDFSClient();
      testFileChecksum();
    } finally {
     noXmlDefaults = false; 
    }
  }

  @Test(timeout=120000)
  public void testLocatedFileStatusStorageIdsTypes() throws Exception {
    final Configuration conf = getTestConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3).build();
    try {
      final DistributedFileSystem fs = cluster.getFileSystem();
      final Path testFile = new Path("/testListLocatedStatus");
      final int blockSize = 4096;
      final int numBlocks = 10;
      // Create a test file
      final int repl = 2;
      DFSTestUtil.createFile(fs, testFile, blockSize, numBlocks * blockSize,
          blockSize, (short) repl, 0xADDED);
      DFSTestUtil.waitForReplication(fs, testFile, (short) repl, 30000);
      // Get the listing
      RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(testFile);
      assertTrue("Expected file to be present", it.hasNext());
      LocatedFileStatus stat = it.next();
      BlockLocation[] locs = stat.getBlockLocations();
      assertEquals("Unexpected number of locations", numBlocks, locs.length);

      Set<String> dnStorageIds = new HashSet<>();
      for (DataNode d : cluster.getDataNodes()) {
        try (FsDatasetSpi.FsVolumeReferences volumes = d.getFSDataset()
            .getFsVolumeReferences()) {
          for (FsVolumeSpi vol : volumes) {
            dnStorageIds.add(vol.getStorageID());
          }
        }
      }

      for (BlockLocation loc : locs) {
        String[] ids = loc.getStorageIds();
        // Run it through a set to deduplicate, since there should be no dupes
        Set<String> storageIds = new HashSet<>();
        Collections.addAll(storageIds, ids);
        assertEquals("Unexpected num storage ids", repl, storageIds.size());
        // Make sure these are all valid storage IDs
        assertTrue("Unknown storage IDs found!", dnStorageIds.containsAll
            (storageIds));
        // Check storage types are the default, since we didn't set any
        StorageType[] types = loc.getStorageTypes();
        assertEquals("Unexpected num storage types", repl, types.length);
        for (StorageType t: types) {
          assertEquals("Unexpected storage type", StorageType.DEFAULT, t);
        }
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Tests the normal path of batching up BlockLocation[]s to be passed to a
   * single
   * {@link DistributedFileSystem#getFileBlockStorageLocations(java.util.List)}
   * call
   */
  @Test(timeout=60000)
  public void testGetFileBlockStorageLocationsBatching() throws Exception {
    final Configuration conf = getTestConfiguration();
    GenericTestUtils.setLogLevel(ProtobufRpcEngine.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(BlockStorageLocationUtil.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.TRACE);

    conf.setBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
        true);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
    try {
      final DistributedFileSystem fs = cluster.getFileSystem();
      // Create two files
      final Path tmpFile1 = new Path("/tmpfile1.dat");
      final Path tmpFile2 = new Path("/tmpfile2.dat");
      DFSTestUtil.createFile(fs, tmpFile1, 1024, (short) 2, 0xDEADDEADl);
      DFSTestUtil.createFile(fs, tmpFile2, 1024, (short) 2, 0xDEADDEADl);
      // Make sure files are fully replicated before continuing
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            List<BlockLocation> list = Lists.newArrayList();
            list.addAll(Arrays.asList(fs.getFileBlockLocations(tmpFile1, 0,
                1024)));
            list.addAll(Arrays.asList(fs.getFileBlockLocations(tmpFile2, 0,
                1024)));
            int totalRepl = 0;
            for (BlockLocation loc : list) {
              totalRepl += loc.getHosts().length;
            }
            if (totalRepl == 4) {
              return true;
            }
          } catch(IOException e) {
            // swallow
          }
          return false;
        }
      }, 500, 30000);
      // Get locations of blocks of both files and concat together
      BlockLocation[] blockLocs1 = fs.getFileBlockLocations(tmpFile1, 0, 1024);
      BlockLocation[] blockLocs2 = fs.getFileBlockLocations(tmpFile2, 0, 1024);
      BlockLocation[] blockLocs = (BlockLocation[]) ArrayUtils.addAll(blockLocs1,
          blockLocs2);
      // Fetch VolumeBlockLocations in batch
      BlockStorageLocation[] locs = fs.getFileBlockStorageLocations(Arrays
          .asList(blockLocs));
      int counter = 0;
      // Print out the list of ids received for each block
      for (BlockStorageLocation l : locs) {
        for (int i = 0; i < l.getVolumeIds().length; i++) {
          VolumeId id = l.getVolumeIds()[i];
          String name = l.getNames()[i];
          if (id != null) {
            System.out.println("Datanode " + name + " has block " + counter
                + " on volume id " + id.toString());
          }
        }
        counter++;
      }
      assertEquals("Expected two HdfsBlockLocations for two 1-block files", 2,
          locs.length);
      for (BlockStorageLocation l : locs) {
        assertEquals("Expected two replicas for each block", 2,
            l.getVolumeIds().length);
        for (int i = 0; i < l.getVolumeIds().length; i++) {
          VolumeId id = l.getVolumeIds()[i];
          String name = l.getNames()[i];
          assertTrue("Expected block to be valid on datanode " + name,
              id != null);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Tests error paths for
   * {@link DistributedFileSystem#getFileBlockStorageLocations(java.util.List)}
   */
  @Test(timeout=60000)
  public void testGetFileBlockStorageLocationsError() throws Exception {
    final Configuration conf = getTestConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_HDFS_BLOCKS_METADATA_ENABLED,
        true);
    conf.setInt(
        DFSConfigKeys.DFS_CLIENT_FILE_BLOCK_STORAGE_LOCATIONS_TIMEOUT_MS, 1500);
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.getDataNodes();
      final DistributedFileSystem fs = cluster.getFileSystem();
      
      // Create a few files and add together their block locations into
      // a list.
      final Path tmpFile1 = new Path("/errorfile1.dat");
      final Path tmpFile2 = new Path("/errorfile2.dat");

      DFSTestUtil.createFile(fs, tmpFile1, 1024, (short) 2, 0xDEADDEADl);
      DFSTestUtil.createFile(fs, tmpFile2, 1024, (short) 2, 0xDEADDEADl);

      // Make sure files are fully replicated before continuing
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            List<BlockLocation> list = Lists.newArrayList();
            list.addAll(Arrays.asList(fs.getFileBlockLocations(tmpFile1, 0,
                1024)));
            list.addAll(Arrays.asList(fs.getFileBlockLocations(tmpFile2, 0,
                1024)));
            int totalRepl = 0;
            for (BlockLocation loc : list) {
              totalRepl += loc.getHosts().length;
            }
            if (totalRepl == 4) {
              return true;
            }
          } catch(IOException e) {
            // swallow
          }
          return false;
        }
      }, 500, 30000);
      
      BlockLocation[] blockLocs1 = fs.getFileBlockLocations(tmpFile1, 0, 1024);
      BlockLocation[] blockLocs2 = fs.getFileBlockLocations(tmpFile2, 0, 1024);

      List<BlockLocation> allLocs = Lists.newArrayList();
      allLocs.addAll(Arrays.asList(blockLocs1));
      allLocs.addAll(Arrays.asList(blockLocs2));

      // Stall on the DN to test the timeout
      DataNodeFaultInjector injector = Mockito.mock(DataNodeFaultInjector.class);
      Mockito.doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          Thread.sleep(3000);
          return null;
        }
      }).when(injector).getHdfsBlocksMetadata();
      DataNodeFaultInjector.instance = injector;

      BlockStorageLocation[] locs = fs.getFileBlockStorageLocations(allLocs);
      for (BlockStorageLocation loc: locs) {
        assertEquals(
            "Found more than 0 cached hosts although RPCs supposedly timed out",
            0, loc.getCachedHosts().length);
      }

      // Restore a default injector
      DataNodeFaultInjector.instance = new DataNodeFaultInjector();

      // Stop a datanode to simulate a failure.
      DataNodeProperties stoppedNode = cluster.stopDataNode(0);
      
      // Fetch VolumeBlockLocations
      locs = fs.getFileBlockStorageLocations(allLocs);
      assertEquals("Expected two HdfsBlockLocation for two 1-block files", 2,
          locs.length);
  
      for (BlockStorageLocation l : locs) {
        assertEquals("Expected two replicas for each block", 2,
            l.getHosts().length);
        assertEquals("Expected two VolumeIDs for each block", 2,
            l.getVolumeIds().length);
        assertTrue("Expected one valid and one invalid volume",
            (l.getVolumeIds()[0] == null) ^ (l.getVolumeIds()[1] == null));
      }
      
      // Start the datanode again, and remove one of the blocks.
      // This is a different type of failure where the block itself
      // is invalid.
      cluster.restartDataNode(stoppedNode, true /*keepPort*/);
      cluster.waitActive();
      
      fs.delete(tmpFile2, true);
      HATestUtil.waitForNNToIssueDeletions(cluster.getNameNode());
      cluster.triggerHeartbeats();
      HATestUtil.waitForDNDeletions(cluster);
  
      locs = fs.getFileBlockStorageLocations(allLocs);
      assertEquals("Expected two HdfsBlockLocations for two 1-block files", 2,
          locs.length);
      assertNotNull(locs[0].getVolumeIds()[0]);
      assertNotNull(locs[0].getVolumeIds()[1]);
      assertNull(locs[1].getVolumeIds()[0]);
      assertNull(locs[1].getVolumeIds()[1]);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testCreateWithCustomChecksum() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    Path testBasePath = new Path("/test/csum");
    // create args 
    Path path1 = new Path(testBasePath, "file_wtih_crc1");
    Path path2 = new Path(testBasePath, "file_with_crc2");
    ChecksumOpt opt1 = new ChecksumOpt(DataChecksum.Type.CRC32C, 512);
    ChecksumOpt opt2 = new ChecksumOpt(DataChecksum.Type.CRC32, 512);

    // common args
    FsPermission perm = FsPermission.getDefault().applyUMask(
        FsPermission.getUMask(conf));
    EnumSet<CreateFlag> flags = EnumSet.of(CreateFlag.OVERWRITE,
        CreateFlag.CREATE);
    short repl = 1;

    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem dfs = cluster.getFileSystem();

      dfs.mkdirs(testBasePath);

      // create two files with different checksum types
      FSDataOutputStream out1 = dfs.create(path1, perm, flags, 4096, repl,
          131072L, null, opt1);
      FSDataOutputStream out2 = dfs.create(path2, perm, flags, 4096, repl,
          131072L, null, opt2);

      for (int i = 0; i < 1024; i++) {
        out1.write(i);
        out2.write(i);
      }
      out1.close();
      out2.close();

      // the two checksums must be different.
      MD5MD5CRC32FileChecksum sum1 =
          (MD5MD5CRC32FileChecksum)dfs.getFileChecksum(path1);
      MD5MD5CRC32FileChecksum sum2 =
          (MD5MD5CRC32FileChecksum)dfs.getFileChecksum(path2);
      assertFalse(sum1.equals(sum2));

      // check the individual params
      assertEquals(DataChecksum.Type.CRC32C, sum1.getCrcType());
      assertEquals(DataChecksum.Type.CRC32,  sum2.getCrcType());

    } finally {
      if (cluster != null) {
        cluster.getFileSystem().delete(testBasePath, true);
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=60000)
  public void testFileCloseStatus() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    try {
      // create a new file.
      Path file = new Path("/simpleFlush.dat");
      FSDataOutputStream output = fs.create(file);
      // write to file
      output.writeBytes("Some test data");
      output.flush();
      assertFalse("File status should be open", fs.isFileClosed(file));
      output.close();
      assertTrue("File status should be closed", fs.isFileClosed(file));
    } finally {
      cluster.shutdown();
    }
  }
  
  @Test(timeout=60000)
  public void testListFiles() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    
    try {
      DistributedFileSystem fs = cluster.getFileSystem();
  
      final Path relative = new Path("relative");
      fs.create(new Path(relative, "foo")).close();
  
      final List<LocatedFileStatus> retVal = new ArrayList<>();
      final RemoteIterator<LocatedFileStatus> iter =
          fs.listFiles(relative, true);
      while (iter.hasNext()) {
        retVal.add(iter.next());
      }
      System.out.println("retVal = " + retVal);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=10000)
  public void testDFSClientPeerReadTimeout() throws IOException {
    final int timeout = 1000;
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);

    // only need cluster to create a dfs client to get a peer
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();     
      DistributedFileSystem dfs = cluster.getFileSystem();
      // use a dummy socket to ensure the read timesout
      ServerSocket socket = new ServerSocket(0);
      Peer peer = dfs.getClient().newConnectedPeer(
          (InetSocketAddress) socket.getLocalSocketAddress(), null, null);
      long start = Time.now();
      try {
        peer.getInputStream().read();
        Assert.fail("read should timeout");
      } catch (SocketTimeoutException ste) {
        long delta = Time.now() - start;
        if (delta < timeout*0.9) {
          throw new IOException("read timedout too soon in " + delta + " ms.",
              ste);
        }
        if (delta > timeout*1.1) {
          throw new IOException("read timedout too late in " + delta + " ms.",
              ste);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=60000)
  public void testGetServerDefaults() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      FsServerDefaults fsServerDefaults = dfs.getServerDefaults();
      Assert.assertNotNull(fsServerDefaults);
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout=10000)
  public void testDFSClientPeerWriteTimeout() throws IOException {
    final int timeout = 1000;
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, timeout);

    // only need cluster to create a dfs client to get a peer
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      cluster.waitActive();
      DistributedFileSystem dfs = cluster.getFileSystem();
      // Write 10 MB to a dummy socket to ensure the write times out
      ServerSocket socket = new ServerSocket(0);
      Peer peer = dfs.getClient().newConnectedPeer(
        (InetSocketAddress) socket.getLocalSocketAddress(), null, null);
      long start = Time.now();
      try {
        byte[] buf = new byte[10 * 1024 * 1024];
        peer.getOutputStream().write(buf);
        long delta = Time.now() - start;
        Assert.fail("write finish in " + delta + " ms" + "but should timedout");
      } catch (SocketTimeoutException ste) {
        long delta = Time.now() - start;

        if (delta < timeout * 0.9) {
          throw new IOException("write timedout too soon in " + delta + " ms.",
              ste);
        }
        if (delta > timeout * 1.2) {
          throw new IOException("write timedout too late in " + delta + " ms.",
              ste);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test(timeout = 30000)
  public void testTotalDfsUsed() throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fs = cluster.getFileSystem();
      // create file under root
      FSDataOutputStream File1 = fs.create(new Path("/File1"));
      File1.write("hi".getBytes());
      File1.close();
      // create file under sub-folder
      FSDataOutputStream File2 = fs.create(new Path("/Folder1/File2"));
      File2.write("hi".getBytes());
      File2.close();
      // getUsed(Path) should return total len of all the files from a path
      assertEquals(2, fs.getUsed(new Path("/Folder1")));
      //getUsed() should return total length of all files in filesystem
      assertEquals(4, fs.getUsed());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  @Test
  public void testDFSCloseFilesBeingWritten() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      DistributedFileSystem fileSys = cluster.getFileSystem();

      // Create one file then delete it to trigger the FileNotFoundException
      // when closing the file.
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.delete(new Path("/test/dfsclose/file-0"), true);

      DFSClient dfsClient = fileSys.getClient();
      // Construct a new dfsClient to get the same LeaseRenewer instance,
      // to avoid the original client being added to the leaseRenewer again.
      DFSClient newDfsClient =
          new DFSClient(cluster.getFileSystem(0).getUri(), conf);
      LeaseRenewer leaseRenewer = newDfsClient.getLeaseRenewer();

      dfsClient.closeAllFilesBeingWritten(false);
      // Remove new dfsClient in leaseRenewer
      leaseRenewer.closeClient(newDfsClient);

      // The list of clients corresponding to this renewer should be empty
      assertEquals(true, leaseRenewer.isEmpty());
      assertEquals(true, dfsClient.isFilesBeingWrittenEmpty());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
