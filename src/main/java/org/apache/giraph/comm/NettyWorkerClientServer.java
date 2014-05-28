/*
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

package org.apache.giraph.comm;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.giraph.graph.partition.Partition;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Netty based implementation of the {@link WorkerClientServer} interface.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
@SuppressWarnings("rawtypes")
public class NettyWorkerClientServer<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable>
    implements WorkerClientServer<I, V, E, M> {
  /** Client that sends requests */
  private final WorkerClient<I, V, E, M> client;
  /** Server that processes requests */
  private final WorkerServer<I, V, E, M> server;

  /**
   * Constructor.
   *
   * @param context Mapper context
   * @param service Service for partition lookup
   */
  public NettyWorkerClientServer(Mapper<?, ?, ?, ?>.Context context,
      CentralizedServiceWorker<I, V, E, M> service) {
    client = new NettyWorkerClient<I, V, E, M>(context, service);
    server = new NettyWorkerServer<I, V, E, M>(context.getConfiguration(),
                                               service);
  }

  @Override
  public void fixPartitionIdToSocketAddrMap() {
    client.fixPartitionIdToSocketAddrMap();
  }

  @Override
  public void sendMessageReq(I destVertexId, M message) {
    client.sendMessageReq(destVertexId, message);
  }

  @Override
  public void sendPartitionReq(WorkerInfo workerInfo,
      Partition<I, V, E, M> partition) {
    client.sendPartitionReq(workerInfo, partition);
  }

  @Override
  public void addEdgeReq(I vertexIndex, Edge<I, E> edge) throws IOException {
    client.addEdgeReq(vertexIndex, edge);
  }

  @Override
  public void removeEdgeReq(I vertexIndex,
                            I destinationVertexIndex) throws IOException {
    client.removeEdgeReq(vertexIndex, destinationVertexIndex);
  }

  @Override
  public void addVertexReq(BasicVertex<I, V, E, M> vertex) throws IOException {
    client.addVertexReq(vertex);
  }

  @Override
  public void removeVertexReq(I vertexIndex) throws IOException {
    client.removeVertexReq(vertexIndex);
  }

  @Override
  public void flush() throws IOException {
    client.flush();
  }

  @Override
  public long resetMessageCount() {
    return client.resetMessageCount();
  }

  @Override
  public void closeConnections() throws IOException {
    client.closeConnections();
  }

  @Override
  public void setup() {
    client.fixPartitionIdToSocketAddrMap();
  }

  @Override
  public void prepareSuperstep() {
    server.prepareSuperstep();
  }

  @Override
  public Map<Integer, Collection<BasicVertex<I, V, E, M>>>
  getInPartitionVertexMap() {
    return server.getInPartitionVertexMap();
  }

  @Override
  public void close() {
    server.close();
  }


  @Override
  public int getPort() {
    return server.getPort();
  }
}
