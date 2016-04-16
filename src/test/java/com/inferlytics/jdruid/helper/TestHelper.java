package com.inferlytics.jdruid.helper;

import com.inferlytics.druidlet.core.DruidIndices;
import com.inferlytics.druidlet.loader.Loader;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.aggregation.*;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.DimFilters;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.spec.QuerySegmentSpecs;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Provides helper methods for testing
 *
 * @author Sriram
 * @since 4/16/2016
 */
public class TestHelper {
    private static final DruidIndices indices = DruidIndices.getInstance();

    public static QueryableIndex getIndex(String indexKey) throws IOException {
        if (indices.has(indexKey)) {
            return indices.get(indexKey);
        }
        //  Create druid segments from raw data
        Reader reader = new FileReader(new File("./src/test/resources/report.csv"));

        List<String> columns = Arrays.asList("colo", "pool", "report", "URL", "TS", "metric", "value", "count", "min", "max", "sum");
        List<String> metrics = Arrays.asList("value", "count", "min", "max", "sum");
        List<String> dimensions = new ArrayList<>(columns);
        dimensions.removeAll(metrics);
        Loader loader = Loader.csv(reader, columns, dimensions, "TS");

        DimensionsSpec dimensionsSpec = new DimensionsSpec(dimensions, null, null);
        AggregatorFactory[] metricsAgg = new AggregatorFactory[]{
                new LongSumAggregatorFactory("agg_count", "count"),
                new DoubleMaxAggregatorFactory("agg_max", "max"),
                new DoubleMinAggregatorFactory("agg_min", "min"),
                new DoubleSumAggregatorFactory("agg_sum", "sum")
        };
        IncrementalIndexSchema indexSchema = new IncrementalIndexSchema(0, QueryGranularity.ALL, dimensionsSpec, metricsAgg);
        indices.cache(indexKey, loader, indexSchema);
        return indices.get(indexKey);
    }

    public static Query getGroupByQuery() {
        List<DimFilter> filters = new ArrayList<>();
        filters.add(DimFilters.dimEquals("report", "URLTransaction"));
        filters.add(DimFilters.dimEquals("pool", "r1cart"));
        filters.add(DimFilters.dimEquals("metric", "Duration"));
        return GroupByQuery.builder()
                .setDataSource("test")
                .setQuerySegmentSpec(QuerySegmentSpecs.create(new Interval(0, new DateTime().getMillis())))
                .setGranularity(QueryGranularity.NONE)
                .addDimension("URL")
                .addAggregator(new LongSumAggregatorFactory("agg_count", "agg_count"))
                .addAggregator(new DoubleMaxAggregatorFactory("agg_max", "agg_max"))
                .addAggregator(new DoubleMinAggregatorFactory("agg_min", "agg_min"))
                .addAggregator(new DoubleSumAggregatorFactory("agg_sum", "agg_sum"))
                .setDimFilter(DimFilters.and(filters))
                .build();
    }

    public static Query getTopNQuery() {
        List<DimFilter> filters = new ArrayList<>();
        filters.add(DimFilters.dimEquals("report", "URLTransaction"));
        filters.add(DimFilters.dimEquals("pool", "r1cart"));
        filters.add(DimFilters.dimEquals("metric", "Duration"));
        return new TopNQueryBuilder()
                .threshold(5)
                .metric("agg_count")
                .dataSource("test")
                .intervals(QuerySegmentSpecs.create(new Interval(0, new DateTime().getMillis())))
                .granularity(QueryGranularity.NONE)
                .dimension("colo")
                .aggregators(
                        Arrays.asList(
                                new LongSumAggregatorFactory("agg_count", "agg_count"),
                                new DoubleMaxAggregatorFactory("agg_max", "agg_max"),
                                new DoubleMinAggregatorFactory("agg_min", "agg_min"),
                                new DoubleSumAggregatorFactory("agg_sum", "agg_sum")))
                .filters(DimFilters.and(filters)).build();
    }

    public static Query getTimeseriesQuery() {
        List<DimFilter> filters = new ArrayList<>();
        filters.add(DimFilters.dimEquals("report", "URLTransaction"));
        filters.add(DimFilters.dimEquals("pool", "r1cart"));
        filters.add(DimFilters.dimEquals("metric", "Duration"));
        return Druids.newTimeseriesQueryBuilder()
                .dataSource("test")
                .intervals(QuerySegmentSpecs.create(new Interval(0, new DateTime().getMillis())))
                .granularity(QueryGranularity.ALL)
                .aggregators(Arrays.asList(
                        new LongSumAggregatorFactory("agg_count", "agg_count"),
                        new DoubleMaxAggregatorFactory("agg_max", "agg_max"),
                        new DoubleMinAggregatorFactory("agg_min", "agg_min"),
                        new DoubleSumAggregatorFactory("agg_sum", "agg_sum")))
                .filters(DimFilters.and(filters)).build();
    }
}
