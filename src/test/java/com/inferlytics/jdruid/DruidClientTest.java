package com.inferlytics.jdruid;

import com.inferlytics.druidlet.app.DruidRunner;
import com.inferlytics.jdruid.helper.TestHelper;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.Result;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Tests the Druid Client
 *
 * @author Sriram
 * @since 4/15/2016
 */
public class DruidClientTest {
    private static final int PORT = 10000 + new Random().nextInt(20000);
    private static final String API_URL = "http://localhost:" + PORT + "/druid/v2/";
    private static final DruidRunner druid;
    private static final DruidClient client;

    static {
        try {
            druid = new DruidRunner(PORT, TestHelper.getIndex("dummy"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        client = new DruidClient(API_URL);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        druid.run();
    }

    @Test
    public void testTopN() throws Exception {
        Query query = TestHelper.getTopNQuery();
        List<Result> results = client.topN(query);
        Assert.assertEquals(results.size(), 1);
    }

    @Test
    public void testGroupBy() throws Exception {
        Query query = TestHelper.getGroupByQuery();
        List<Row> results = client.groupBy(query);
        Assert.assertEquals(results.size(), 2);

        if (results.get(0).getDimension("URL").get(0).equals("abc")) {
            Assert.assertEquals(results.get(0).getLongMetric("agg_sum"), 247);
            Assert.assertEquals(results.get(0).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(0).getLongMetric("agg_max"), 124);
            Assert.assertEquals(results.get(0).getLongMetric("agg_count"), 12);
            Assert.assertEquals(results.get(1).getLongMetric("agg_sum"), 123);
            Assert.assertEquals(results.get(1).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(1).getLongMetric("agg_max"), 123);
            Assert.assertEquals(results.get(1).getLongMetric("agg_count"), 3);

        } else {
            Assert.assertEquals(results.get(0).getLongMetric("agg_sum"), 123);
            Assert.assertEquals(results.get(0).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(0).getLongMetric("agg_max"), 123);
            Assert.assertEquals(results.get(0).getLongMetric("agg_count"), 3);
            Assert.assertEquals(results.get(1).getLongMetric("agg_sum"), 247);
            Assert.assertEquals(results.get(1).getLongMetric("agg_min"), 0);
            Assert.assertEquals(results.get(1).getLongMetric("agg_max"), 124);
            Assert.assertEquals(results.get(1).getLongMetric("agg_count"), 12);
        }
    }

    @Test
    public void testTimeseries() throws Exception {
        Query query = TestHelper.getTimeseriesQuery();
        List<Result> results = client.timeseries(query);
        Assert.assertEquals(results.size(), 1);
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) results.get(0).getValue();
        Assert.assertEquals(value.get("agg_sum"), 370.0);
        Assert.assertEquals(value.get("agg_min"), 0.0);
        Assert.assertEquals(value.get("agg_max"), 124.0);
        Assert.assertEquals(value.get("agg_count"), 15);
    }

    @Test
    public void testQuery() throws Exception {
        Query query = TestHelper.getGroupByQuery();
        List<Map<String, Object>> results = client.query(query);
        Assert.assertEquals(results.size(), 2);
        @SuppressWarnings("unchecked")
        Map<String, Object> result0 = (Map<String, Object>) results.get(0).get("event");
        @SuppressWarnings("unchecked")
        Map<String, Object> result1 = (Map<String, Object>) results.get(1).get("event");

        if (result0.get("URL").equals("abc")) {
            Assert.assertEquals(result0.get("agg_sum"), 247.0);
            Assert.assertEquals(result0.get("agg_min"), 0.0);
            Assert.assertEquals(result0.get("agg_max"), 124.0);
            Assert.assertEquals(result0.get("agg_count"), 12);
            Assert.assertEquals(result1.get("agg_sum"), 123.0);
            Assert.assertEquals(result1.get("agg_min"), 0.0);
            Assert.assertEquals(result1.get("agg_max"), 123.0);
            Assert.assertEquals(result1.get("agg_count"), 3);
        } else {
            Assert.assertEquals(result0.get("agg_sum"), 123.0);
            Assert.assertEquals(result0.get("agg_min"), 0.0);
            Assert.assertEquals(result0.get("agg_max"), 123.0);
            Assert.assertEquals(result0.get("agg_count"), 3);
            Assert.assertEquals(result1.get("agg_sum"), 247.0);
            Assert.assertEquals(result1.get("agg_min"), 0.0);
            Assert.assertEquals(result1.get("agg_max"), 124.0);
            Assert.assertEquals(result1.get("agg_count"), 12);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        druid.stop();
    }
}
