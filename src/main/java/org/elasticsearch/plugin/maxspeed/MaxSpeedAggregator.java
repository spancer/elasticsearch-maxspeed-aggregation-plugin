package org.elasticsearch.plugin.maxspeed;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FutureArrays;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Administrator
 *
 */
public class MaxSpeedAggregator extends MetricsAggregator {
  private final static Logger logger = LoggerFactory.getLogger(MaxSpeedAggregator.class);

  private final ArrayValuesSource.MultArrayValuesSource values;

  // hold last bucket record (time,geo), so as to compute speed.
  ArrayValuesSource.MultArrayValuesSource lastValues;

  private DoubleArray lastLat, lastLon, lastTime;
  DoubleArray maxes;



  public MaxSpeedAggregator(String name, Map<String, ValuesSource> valuesSources,
      SearchContext searchContext, Aggregator aggregator, MultiValueMode multiValueMode,
      Map<String, Object> stringObjectMap) throws IOException {
    super(name, searchContext, aggregator, stringObjectMap);
    this.values = valuesSources == null ? null
        : new ArrayValuesSource.MultArrayValuesSource(valuesSources, multiValueMode);
    if (valuesSources != null && !valuesSources.isEmpty()) {
      final BigArrays bigArrays = context.bigArrays();
      maxes = bigArrays.newDoubleArray(1, false);
      maxes.fill(0, maxes.size(), Double.NEGATIVE_INFINITY);
      lastLat = bigArrays.newDoubleArray(1, true);
      lastLon = bigArrays.newDoubleArray(1, true);
      lastTime = bigArrays.newDoubleArray(1, true);
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub)
      throws IOException {
    if (this.values == null) {
      if (parent != null) {
        return LeafBucketCollector.NO_OP_COLLECTOR;
      } else {
        throw new CollectionTerminatedException();
      }
    }
    final BigArrays bigArrays = context.bigArrays();
    final MultiGeoPointValues geoValues = values.getGeo().geoPointValues(ctx);
    final SortedNumericDoubleValues timeValues = values.getTime().doubleValues(ctx);
    final DoubleArray preValue = bigArrays.newDoubleArray(3, true);
    logger.info("fieldNames:{},times:{},geo_docValueCount:{}", values.fieldNames(),
        timeValues.docValueCount(), geoValues.docValueCount());
    return new LeafBucketCollectorBase(sub, values) {
      @Override
      public void collect(int doc, long bucket) throws IOException {
        if (bucket >= maxes.size()) {
          long from = maxes.size();
          maxes = bigArrays.grow(maxes, bucket + 1);
          maxes.fill(from, maxes.size(), Double.NEGATIVE_INFINITY);
        }

        lastLat = bigArrays.grow(lastLat, bucket + 1);
        lastLon = bigArrays.grow(lastLon, bucket + 1);
        lastTime = bigArrays.grow(lastTime, bucket + 1);

        preValue.set(0, lastTime.get(bucket));
        preValue.set(1, lastLat.get(bucket));
        preValue.set(2, lastLon.get(bucket));

        double currentLat = 0;
        double currentLon = 0;
        double currentTime = 0;

        if (geoValues.advanceExact(doc)) {
          final int geoCount = geoValues.docValueCount();
          for (int i = 0; i < geoCount; ++i) {
            GeoPoint value = geoValues.nextValue();
            currentLat = value.getLat();
            currentLon = value.getLon();
          }
        }

        if (timeValues.advanceExact(doc)) {
          final int timeCount = timeValues.docValueCount();
          for (int i = 0; i < timeCount; ++i) {
            currentTime = timeValues.nextValue();
          }
        }

        // calculate speed
        if (preValue.get(1) != 0 && preValue.get(2) != 0) {
          double speed = Double.MIN_VALUE;
          if (currentTime - preValue.get(0) != 0) {
            speed = GeoDistance.ARC.calculate(preValue.get(1), preValue.get(2), currentLat,
                currentLon, DistanceUnit.KILOMETERS)
                / (Math.abs(currentTime - preValue.get(0)) / 1000 * 60 * 60);

          } else
            speed = Double.MAX_VALUE;
          double max = maxes.get(bucket);
          max = Math.max(max, speed);
          logger.info(
              "bucket:{}, preTime:{}, pre lat:{},pre lon:{}, currentTime:{}, currentLat:{}, currentLon:{},lastMax:{}, currentMax:{}",
              bucket, lastTime.get(bucket), lastLat.get(bucket), lastLon.get(bucket), currentTime,
              currentLat, currentLon, maxes.get(bucket), max);
          logger.info("bucket:{}, lastMax:{}, currentMax:{}", bucket, maxes.get(bucket), max);
          maxes.set(bucket, max);

        }

        // set current as previous
        preValue.set(0, currentTime);
        preValue.set(1, currentLat);
        preValue.set(2, currentLon);

        // updated last time, lat, lon info
        lastTime.set(bucket, preValue.get(0));
        lastLat.set(bucket, preValue.get(1));
        lastLon.set(bucket, preValue.get(2));
      }
    };

  }


  @Override
  public InternalAggregation buildAggregation(long bucket) {
    if (values == null || bucket >= maxes.size()) {
      return buildEmptyAggregation();
    }
    return new InternalMax(name, maxes.get(bucket), DocValueFormat.RAW, metadata());
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalMax(name, Double.NEGATIVE_INFINITY, DocValueFormat.RAW, metadata());
  }

  @Override
  public void doClose() {
    Releasables.close(maxes);
  }

  /**
   * Returns the maximum value indexed in the <code>fieldName</code> field or <code>null</code> if
   * the value cannot be inferred from the indexed {@link PointValues}.
   */
  static Number findLeafMaxValue(LeafReader reader, String fieldName,
      Function<byte[], Number> converter) throws IOException {
    final PointValues pointValues = reader.getPointValues(fieldName);
    if (pointValues == null) {
      return null;
    }
    final Bits liveDocs = reader.getLiveDocs();
    if (liveDocs == null) {
      return converter.apply(pointValues.getMaxPackedValue());
    }
    int numBytes = pointValues.getBytesPerDimension();
    final byte[] maxValue = pointValues.getMaxPackedValue();
    final byte[][] result = new byte[1][];
    pointValues.intersect(new PointValues.IntersectVisitor() {
      @Override
      public void visit(int docID) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void visit(int docID, byte[] packedValue) {
        if (liveDocs.get(docID)) {
          // we need to collect all values in this leaf (the sort is ascending) where
          // the last live doc is guaranteed to contain the max value for the segment.
          if (result[0] == null) {
            result[0] = new byte[packedValue.length];
          }
          System.arraycopy(packedValue, 0, result[0], 0, packedValue.length);
        }
      }

      @Override
      public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        if (FutureArrays.equals(maxValue, 0, numBytes, maxPackedValue, 0, numBytes)) {
          // we only check leaves that contain the max value for the segment.
          return PointValues.Relation.CELL_CROSSES_QUERY;
        } else {
          return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
      }
    });
    return result[0] != null ? converter.apply(result[0]) : null;
  }
}
