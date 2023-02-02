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

package org.apache.flink.runtime.util.profiling;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;

/**
 * The MetricsManager is responsible for logging activity profiling information (except for
 * messages). It gathers start and end events for deserialization, processing, serialization,
 * blocking on read and write buffers and records activity durations. There is one MetricsManager
 * instance per Task (operator instance). The MetricsManager aggregates metrics in a {@link
 * ProcessingStatus} object and outputs processing and output rates periodically to a designated
 * rates file.
 */
public class FSMetricsManager implements Serializable, MetricsManager {
    private static final Logger LOG = LoggerFactory.getLogger(FSMetricsManager.class);

    private String taskId; // Flink's task description
    private String workerName; // The task description string logged in the rates file
    private int instanceId; // The operator instance id
    private int numInstances; // The total number of instances for this operator
    private final JobVertexID jobVertexId; // To interact with StreamSwitch

    private long recordsIn = 0; // Total number of records ingested since the last flush
    private long recordsOut = 0; // Total number of records produced since the last flush
    private long usefulTime = 0; // Total period of useful time since last flush
    private long waitingTime = 0; // Total waiting time for input/output buffers since last flush
    private long latency = 0; // Total end to end latency

    private long totalRecordsIn = 0; // Total number of records ingested since the last flush
    private long totalRecordsOut = 0; // Total number of records produced since the last flush

    private long currentWindowStart;

    private final ProcessingStatus status;

    private final long windowSize; // The aggregation interval
    //	private final String ratesPath;	// The file path where to output aggregated rates

    private long epoch =
            0; // The current aggregation interval. The MetricsManager outputs one rates file per
    // epoch.

    private int nRecords;

    private final int numKeygroups;

    private HashMap<Integer, Long> kgLatencyMap = new HashMap<>(); // keygroup -> avgLatency
    private HashMap<Integer, Integer> kgNRecordsMap = new HashMap<>(); // keygroup -> nRecords
    private long lastTimeSlot = 0l;

    private final OutputStreamDecorator outputStreamDecorator;

    /**
     * @param taskDescription the String describing the owner operator instance
     * @param jobConfiguration this job's configuration
     */
    public FSMetricsManager(
            String taskDescription,
            JobVertexID jobVertexId,
            Configuration jobConfiguration,
            int idInModel,
            int maximumKeygroups) {
        numKeygroups = maximumKeygroups;

        taskId = taskDescription;
        String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
        workerName = workerId.substring(0, workerId.indexOf("(") - 1);
        instanceId =
                Integer.parseInt(
                                workerId.substring(
                                        workerId.lastIndexOf("(") + 1, workerId.lastIndexOf("/")))
                        - 1; // need to consistent with partitionAssignment
        instanceId = idInModel;
        //		System.out.println("----updated task with instance id is: " + workerName + "-" +
        // instanceId);
        System.out.println(
                "start execution: "
                        + workerName
                        + "-"
                        + instanceId
                        + " time: "
                        + System.currentTimeMillis());
        numInstances =
                Integer.parseInt(
                        workerId.substring(
                                workerId.lastIndexOf("/") + 1, workerId.lastIndexOf(")")));
        status = new ProcessingStatus();

        windowSize = jobConfiguration.getLong("policy.windowSize", 1_000_000_000L);
        nRecords = jobConfiguration.getInteger("policy.metrics.nrecords", 15);

        currentWindowStart = status.getProcessingStart();

        this.jobVertexId = jobVertexId;

        OutputStream outputStream;
        try {
            String expDir = jobConfiguration.getString("trisk.exp.dir", "/data/flink");
            outputStream = new FileOutputStream(expDir + "/trisk/" + getJobVertexId() + ".output");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            outputStream = System.out;
        }
        LOG.info("###### " + getJobVertexId() + " new task created");
        outputStreamDecorator = new OutputStreamDecorator(outputStream);
    }

    @Override
    public void updateTaskId(String taskDescription, Integer idInModel) {}

    @Override
    public void newInputBuffer(long timestamp) {}

    @Override
    public String getJobVertexId() {
        return null;
    }

    @Override
    public void addSerialization(long serializationDuration) {}

    @Override
    public void addDeserialization(long deserializationDuration) {}

    @Override
    public void incRecordsOut() {}

    @Override
    public void incRecordsOutKeyGroup(int targetKeyGroup) {}

    @Override
    public void incRecordIn(int keyGroup) {}

    @Override
    public void addWaitingForWriteBufferDuration(long duration) {}

    @Override
    public void inputBufferConsumed(
            long timestamp,
            long deserializationDuration,
            long processing,
            long numRecords,
            long endToEndLatency) {}

    @Override
    public void groundTruth(int keyGroup, long arrivalTs, long completionTs) {}

    @Override
    public void groundTruth(long arrivalTs, long latency) {}

    @Override
    public void outputBufferFull(long timestamp) {}

    @Override
    public void updateMetrics() {}
}
