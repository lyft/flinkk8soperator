package com.lyft;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorTestApp {
  public static class StreamingImpulseSource extends RichParallelSourceFunction<Long> implements
      ListCheckpointed<Long> {
    Logger log = LoggerFactory.getLogger(StreamingImpulseSource.class);

    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private long count = 0;
    private final int intervalMillis;

    public StreamingImpulseSource(int intervalMillis) {
      this.intervalMillis = intervalMillis;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws IOException {
      while (!cancelled.get()) {
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(count++);
        }

        try {
          if (intervalMillis > 0) {
            Thread.sleep(intervalMillis);
          }
        } catch (InterruptedException e) {
          // pass
        }
      }

    }

    @Override
    public void cancel() {
      this.cancelled.set(true);
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
      File file = new File("/checkpoints/checkpoint_delay");
      if (file.exists()) {
        String checkpointDelay = new String(Files.readAllBytes(file.toPath()))
            .replaceAll("\n", "");
        int delay = Integer.valueOf(checkpointDelay);
        log.info("Waiting {} milliseconds", delay);
        System.out.println(String.format("PRINT Waiting %d milliseconds", delay));

        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          log.error("Interrupted", e);
        }
      }

      return Collections.singletonList(count);
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
      if (!state.isEmpty()) {
        count = state.get(0);
      }
    }
  }

  public static class MaybeFail implements MapFunction<Long, Long> {

    @Override
    public Long map(Long x) throws Exception {
      if (new File("/checkpoints/fail").exists()) {
        throw new RuntimeException("FAILED!!!");
      }

      return x;
    }
  }

  public static void main(String[] args) throws Exception {
    Logger log = LoggerFactory.getLogger(OperatorTestApp.class);

    log.info("Submitting job...");

    String uid = "default";
    if (args.length > 0) {
      uid = args[0];
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    configureEnvironment(env);

    SingleOutputStreamOperator<Long> dataStream = env
        .addSource(new StreamingImpulseSource(1000))
        .map(new MaybeFail())
        .map(x -> Tuple2.of(0, x))
        .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>(){}))
        .keyBy(0)
        .timeWindow(Time.seconds(10))
        .max(1)
        .uid(uid)
        .map(x -> x.f1);

    dataStream.print();

    env.execute("Window Count");
  }

  private static void configureEnvironment(StreamExecutionEnvironment env) {
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setCheckpointTimeout(10_000);
    env.enableCheckpointing(5_000);
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    if (System.getenv("EXTERNAL_CHECKPOINT") != null) {
      env.getCheckpointConfig()
          .enableExternalizedCheckpoints(
              CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }

    // It is normally safe to use this setting and it can be a big performance improvement as it
    // skips a per-event serializer copy.  The caveat is that you must treat your data objects as
    // immutable.
    env.getConfig().enableObjectReuse();
  }
}
