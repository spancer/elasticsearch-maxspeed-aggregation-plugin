package org.elasticsearch.plugin.maxspeed;

import static java.util.Collections.singletonList;

import java.util.List;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.metrics.InternalMax;

/**
 * @ClassName UmxDistanceAggregationPlugin
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */
public class MaxSpeedAggregationPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<SearchPlugin.AggregationSpec> getAggregations() {
        return singletonList(new SearchPlugin.AggregationSpec(MaxspeedAggregationBuilder.NAME, MaxspeedAggregationBuilder::new,
                new DistanceParser()).addResultReader(InternalMax::new));
    }
}
