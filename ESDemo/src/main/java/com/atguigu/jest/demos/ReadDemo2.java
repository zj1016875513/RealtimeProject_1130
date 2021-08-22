package com.atguigu.jest.demos;

import com.atguigu.jest.beans.Employee;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.AvgAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Created by VULCAN on 2021/4/28
 */
public class ReadDemo2 {

    public static void main(String[] args) throws IOException {


        // ①创建客户端
        JestClientFactory jestClientFactory = new JestClientFactory();

        //为JestClientFactory的HttpClientConfig赋值
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        JestClient jestClient = jestClientFactory.getObject();

        //使用面向对象的方式封装查询条件
        // 封装 query部分   "match": {"hobby": "购物"}
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("hobby", "购物");

        // 封装aggs部分      empnums: {terms   {avgname     avg  }}
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders
                .terms("empnums")
                .field("gender.keyword")
                .size(10)
                .subAggregation(AggregationBuilders.avg("avgname").field("age"));


        //原先：String query=xxx  现在： 工具类，将之前构造的各种对象，生成一个查询的字符串
        String query = new SearchSourceBuilder().query(matchQueryBuilder).aggregation(aggregationBuilder).toString();

        // 构造Search
        Search search = new Search.Builder(query).build();

        //得到查询结果
        SearchResult searchResult = jestClient.execute(search);

        //从结果中取出感兴趣的数据
        //取 hits.total
        System.out.println(searchResult.getTotal());

        //取hits.max_score
        System.out.println(searchResult.getMaxScore());

        //取数据   hits.hits
        List<SearchResult.Hit<Employee, Void>> hits = searchResult.getHits(Employee.class);

        for (SearchResult.Hit<Employee, Void> hit : hits) {

            //取_id
            System.out.println(hit.id);
            System.out.println(hit.type);
            System.out.println(hit.source);
            System.out.println(hit.index);

            System.out.println("-------取数据----------------");
            System.out.println(hit.source);

        }

        //如果有聚合的结果，也可以取出聚合的结果   aggregations
        MetricAggregation aggregations = searchResult.getAggregations();

        // 聚合的结果在buckets中
        TermsAggregation empnums = aggregations.getTermsAggregation("empnums");

        List<TermsAggregation.Entry> buckets = empnums.getBuckets();

        for (TermsAggregation.Entry bucket : buckets) {

            System.out.println(bucket.getKey() + ":" + bucket.getCount());

            //取子聚合
            AvgAggregation avgname = bucket.getAvgAggregation("avgname");

            System.out.println(avgname.getAvg());

        }

        jestClient.close();
    }
}
