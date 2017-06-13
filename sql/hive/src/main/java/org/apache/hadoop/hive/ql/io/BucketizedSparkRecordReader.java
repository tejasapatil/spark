package org.apache.hadoop.hive.ql.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.io.HiveIOExceptionHandlerUtil;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BucketizedSparkRecordReader<K extends WritableComparable, V extends Writable>
        implements RecordReader<K, V> {

  private static final Log LOG = LogFactory.getLog(BucketizedSparkRecordReader.class.getName());

  protected final BucketizedHiveInputSplit split;
  protected final InputFormat inputFormat;
  protected final Reporter reporter;
  protected long progress;
  protected int idx;

  protected RecordReader recordReader;
  protected JobConf jobConf;

  public BucketizedSparkRecordReader(
      InputFormat inputFormat,
      BucketizedHiveInputSplit bucketizedSplit,
      JobConf jobConf,
      Reporter reporter) throws IOException {
    this.recordReader = null;
    this.jobConf = jobConf;
    this.split = bucketizedSplit;
    this.inputFormat = inputFormat;
    this.reporter = reporter;
    initNextRecordReader();
  }

  /**
   * Get the record reader for the next chunk in this
   * BucketizedSparkRecordReader.
   */
  private boolean initNextRecordReader() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
      if (idx > 0) {
        progress += split.getLength(idx - 1); // done processing so far
      }
    }

    // if all chunks have been processed, nothing more to do.
    if (idx == split.getNumSplits()) {
      return false;
    }

    // get a record reader for the idx-th chunk
    try {
      recordReader = inputFormat.getRecordReader(split.getSplit(idx), jobConf, reporter);
    } catch (Exception e) {
      recordReader = HiveIOExceptionHandlerUtil.handleRecordReaderCreationException(e, jobConf);
    }

    idx++;
    return true;
  }

  @Override
  public boolean next(K key, V value) throws IOException {
    try {
      while ((recordReader == null) || !doNextWithExceptionHandler(key, value)) {
        if (!initNextRecordReader()) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      throw e;
    }
  }

  private boolean doNextWithExceptionHandler(K key, V value) throws IOException {
    try {
      return recordReader.next(key,  value);
    } catch (Exception e) {
      return HiveIOExceptionHandlerUtil.handleRecordReaderNextException(e, jobConf);
    }
  }

  @Override
  public K createKey() {
    return (K) recordReader.createKey();
  }

  @Override
  public V createValue() {
    return (V) recordReader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    if (recordReader != null) {
      return recordReader.getPos();
    } else {
      return 0;
    }
  }

  @Override
  public void close() throws IOException {
    if (recordReader != null) {
      recordReader.close();
      recordReader = null;
    }
    idx = 0;
  }

  @Override
  public float getProgress() throws IOException {
    // The calculation is strongly dependent on the assumption that all splits
    // came from the same file
    return Math.min(1.0f, (recordReader == null ?
            progress : recordReader.getPos()) / (float) (split.getLength()));
  }
}
