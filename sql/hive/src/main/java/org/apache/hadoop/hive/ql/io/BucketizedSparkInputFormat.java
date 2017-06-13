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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

public class BucketizedSparkInputFormat<K extends WritableComparable, V extends Writable>
        extends BucketizedHiveInputFormat<K, V> {

  @Override
  public RecordReader getRecordReader(
      InputSplit split,
      JobConf job,
      Reporter reporter) throws IOException {

    BucketizedHiveInputSplit hsplit = (BucketizedHiveInputSplit) split;
    String inputFormatClassName = null;
    Class inputFormatClass = null;

    try {
      inputFormatClassName = hsplit.inputFormatClassName();
      inputFormatClass = job.getClassByName(inputFormatClassName);
    } catch (ClassNotFoundException e) {
      throw new IOException("Cannot find class " + inputFormatClassName, e);
    }

    InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
    return new BucketizedSparkRecordReader<>(inputFormat, hsplit, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numBuckets) throws IOException {
    final String inputFormatClassName = job.get("file.inputformat");
    final String[] inputDirs = job.get(INPUT_DIR).split(StringUtils.COMMA_STR);
    final String[] partitionValues =
      job.get("bucketized.input.format.partition.values").split(StringUtils.COMMA_STR);

    final ArrayList<ArrayList<InputSplit>> splitsForAllBuckets =
      new ArrayList<>(inputDirs.length);

    final ArrayList<ArrayList<String>> partitionValuesForAllBuckets =
      new ArrayList<>(inputDirs.length);

    for (int i = 0; i < numBuckets; i++) {
      splitsForAllBuckets.add(new ArrayList<>());
      partitionValuesForAllBuckets.add(new ArrayList<>());
    }

    for (int i = 0; i < inputDirs.length; i++) {
      final String partitionValue = partitionValues[i];
      final Path inputPath = new Path(inputDirs[i]);
      final JobConf newJob = new JobConf(job);
      final FileStatus[] listStatus = this.listStatus(newJob, inputPath);

      if (listStatus.length != 0 && listStatus.length != numBuckets) {
        throw new IOException("Bucketed path was expected to have " + numBuckets + " files but " +
                listStatus.length + " files are present. Path = " + inputPath);
      }

      try {
        final Class<?> inputFormatClass = Class.forName(inputFormatClassName);
        final InputFormat inputFormat = getInputFormatFromCache(inputFormatClass, job);
        newJob.setInputFormat(inputFormat.getClass());

        for (int j = 0; j < numBuckets; j++) {
          final FileStatus fileStatus = listStatus[j];
          LOG.info("block size: " + fileStatus.getBlockSize());
          LOG.info("file length: " + fileStatus.getLen());
          FileInputFormat.setInputPaths(newJob, fileStatus.getPath());

          final InputSplit[] inputSplits = inputFormat.getSplits(newJob, 0);
          if (inputSplits != null && inputSplits.length > 0) {
            for (InputSplit split: inputSplits) {
              splitsForAllBuckets.get(j).add(split);
              partitionValuesForAllBuckets.get(j).add(partitionValue);
            }
          }
        }
      } catch (ClassNotFoundException e) {
        throw new IOException("unable to find the inputformat class " + inputFormatClassName, e);
      }
    }

    final InputSplit[] result = new InputSplit[numBuckets];
    for (int i = 0; i < numBuckets; i++) {
      final ArrayList<InputSplit> splitsForCurrentBucket = splitsForAllBuckets.get(i);
      result[i] =
        new BucketizedSparkInputSplit(
          splitsForCurrentBucket.toArray(new InputSplit[splitsForCurrentBucket.size()]),
          inputFormatClassName,
          partitionValuesForAllBuckets.get(i));
    }

    return result;
  }
}