# jDruid
Simple Java Druid client

##Build Status

**jDruid** is configured on Travis CI. The current status of the master branch is given below:

![](https://travis-ci.org/InferlyticsOSS/jDruid.svg?branch=master)

##Setting it up

###From scratch

You can clone this repository and build the client by using 

	mvn clean package 

This will generate `jdruid-0.1.0.jar` in the `target` folder which can be used in your project.

###Maven

**jDruid** is available on Bintray and Maven Central:

    <dependency>
	    <groupId>com.inferlytics</groupId>
	    <artifactId>jdruid</artifactId>
	    <version>0.1.1</version>
	</dependency>

##Usage

**jDruid** provides the following methods:

1. topN
2. groupBy
3. timeseries
4. query

The first three return results in `List`s with native Druid types. The `query` is a generic "catch all" method, which can run any valid `Query` and returns a `List` of `Map<String, Object>`

It should be fairly simple to use:

	Query query = Druids.newTimeseriesQueryBuilder().build();
    DruidClient client = new DruidClient("http://localhost:8082/druid/v2");
	List<Result> results = client.timeseries(query);

##Extending functionality

The `query()` method should be generic enough to support any valid Druid `Query`, which returns a `List<Map<String, Object>>`, which can be converted to your custom POJO using the either the Jackson `ObjectMapper` provided by Druid, which can be instantiated by calling `new DefaultObjectMapper()`, or use `Gson` or any other valid SerDe provider.

##Help

If you run into any issues while using **jDruid**, please feel free to drop an email to sriram@raremile.com, or raise an [issue](http://github.com/InferlyticsOSS/jDruid/issues).
