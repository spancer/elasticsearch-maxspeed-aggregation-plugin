package org.elasticsearch.plugin.maxspeed;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugin.support.ArrayValuesSourceAggregationBuilder;
import org.elasticsearch.plugin.support.ArrayValuesSourceAggregatorFactory;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName UmxDistanceAggregationBuilder
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/23
 */
public class MaxspeedAggregationBuilder
    extends ArrayValuesSourceAggregationBuilder.LeafOnly<MaxspeedAggregationBuilder> {
  private final static Logger logger = LoggerFactory.getLogger(MaxspeedAggregationBuilder.class);

  public static final String NAME = "maxspeed";
  private MultiValueMode multiValueMode = MultiValueMode.AVG;

  public MaxspeedAggregationBuilder(String name) {
    super(name);
  }

  public MaxspeedAggregationBuilder(MaxspeedAggregationBuilder clone,
      AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
    super(clone, factoriesBuilder, metadata);
    this.multiValueMode = clone.multiValueMode;

  }

  public MaxspeedAggregationBuilder(StreamInput in) throws IOException {
    super(in);
  }

  @Override
  protected void innerWriteTo(StreamOutput out) throws IOException {
    // Do nothing, no extra state to write to stream
  }

  public MaxspeedAggregationBuilder multiValueMode(MultiValueMode multiValueMode) {
    this.multiValueMode = multiValueMode;
    return this;
  }

  public MultiValueMode multiValueMode() {
    return this.multiValueMode;
  }

  @Override
  protected ArrayValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext,
      Map<String, ValuesSourceConfig> configs, AggregatorFactory parent,
      AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
    return new MaxspeedAggregatorFactory(name, multiValueMode, configs, queryShardContext, parent,
        subFactoriesBuilder, metadata);

  }

  @Override
  protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder,
      Map<String, Object> metadata) {
    return new MaxspeedAggregationBuilder(this, factoriesBuilder, metadata);
  }


  @Override
  protected XContentBuilder doXContentBody(XContentBuilder builder, Params params)
      throws IOException {
    return builder;
  }

  @Override
  public String getType() {
    return NAME;
  }
}
