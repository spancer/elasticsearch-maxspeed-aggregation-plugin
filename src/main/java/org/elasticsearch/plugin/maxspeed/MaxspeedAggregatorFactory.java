package org.elasticsearch.plugin.maxspeed;

import com.alibaba.fastjson.JSON;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugin.support.ArrayValuesSourceAggregatorFactory;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName UmxDistanceAggregatorFactory
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/23
 */
public class MaxspeedAggregatorFactory extends ArrayValuesSourceAggregatorFactory {
    private final static Logger logger = LoggerFactory.getLogger(MaxspeedAggregatorFactory.class);

    private final MultiValueMode multiValueMode;

    public MaxspeedAggregatorFactory(String name,  MultiValueMode multiValueMode
                                        ,Map<String, ValuesSourceConfig> configs, QueryShardContext queryShardContext, AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metadata) throws IOException {
        super(name, configs, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.multiValueMode = multiValueMode;

    }

    @Override
    protected Aggregator doCreateInternal(Map<String, ValuesSource> valuesSources, SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
        logger.info("doCreateInternal-valuesSources-size:{}", JSON.toJSONString(valuesSources.size()));
        return new MaxSpeedAggregator(name, valuesSources, searchContext, parent, multiValueMode, metadata);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new MaxSpeedAggregator(name, null, searchContext, parent,multiValueMode, metadata);
    }

}
