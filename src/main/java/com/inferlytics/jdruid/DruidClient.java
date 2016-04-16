package com.inferlytics.jdruid;

import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.Result;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.POST;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Druid client supporting synchronous calls
 *
 * @author Sriram
 * @since 4/15/2016
 */
public class DruidClient {
    private interface Druid {
        @POST(".")
        Call<List<Result>> timeseries(@Body Query query);

        @POST(".")
        Call<List<Result>> topN(@Body Query query);

        @POST(".")
        Call<List<Row>> groupBy(@Body Query query);

        @POST(".")
        Call<List<Map<String, Object>>> query(@Body Query query);
    }

    private Druid druid;

    /**
     * Instantiates a new Druid client pointing at the given URL
     *
     * @param apiUrl Endpoint, in the format: http://druidhost:port/druid/v2
     */
    public DruidClient(String apiUrl) {
        druid = new Retrofit.Builder()
                .baseUrl(apiUrl).addConverterFactory(JacksonConverterFactory.create(new DefaultObjectMapper()))
                .build().create(Druid.class);
    }

    /**
     * Executes a Top N query
     *
     * @param query Query to execute
     * @return List of Result objects
     * @throws IOException Thrown if there was an issue executing the Query
     */
    public List<Result> topN(Query query) throws IOException {
        return druid.topN(query).execute().body();
    }

    /**
     * Executes a Group By query
     *
     * @param query Query to execute
     * @return List of Row objects
     * @throws IOException Thrown if there was an issue executing the Query
     */
    public List<Row> groupBy(Query query) throws IOException {
        return druid.groupBy(query).execute().body();
    }

    /**
     * Executes a Timeseries query
     *
     * @param query Query to execute
     * @return List of Result objects
     * @throws IOException Thrown if there was an issue executing the Query
     */
    public List<Result> timeseries(Query query) throws IOException {
        return druid.timeseries(query).execute().body();
    }

    /**
     * Executes any valid Query on Druid
     *
     * @param query Query to execute
     * @return List of Maps corresponding to the result returned by Druid
     * @throws IOException Thrown if there was an issue executing the Query
     */
    public List<Map<String, Object>> query(Query query) throws IOException {
        return druid.query(query).execute().body();
    }
}
