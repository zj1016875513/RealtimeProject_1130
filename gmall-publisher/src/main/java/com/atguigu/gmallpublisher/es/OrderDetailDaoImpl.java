package com.atguigu.gmallpublisher.es;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.beans.Option;
import com.atguigu.gmallpublisher.beans.SaleDetail;
import com.atguigu.gmallpublisher.beans.Stat;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
//import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by VULCAN on 2021/4/29
 *
 *      {
 *           "total": 62,
 *             "stat":
 *                      [
 *                              ｛
 *                                  "options"： [
 *                                          {},{},{}
 *                                  ],
 *                                  "title": "用户年龄占比"
 *                              ｝
 *                              ｛
 *                                    "options"： [
 *                                          {},{}
 *                                          ],
 *  *                                  "title": "用户性别占比"
 *                              ｝
 *                    ],
 *             "detail":
 *             [
 *                      {},{},{}
 *             ]
 *
 *
 *      }
 */
@Repository
public class OrderDetailDaoImpl implements  OrderDetailDao {

    // 和kafka一样，只要引入spring-boot-starter-data-elasticsearch，容器会自动创建一个JestClient
    @Autowired
    private JestClient jestClient;

    public List<Stat> getStat(SearchResult searchResult){

        ArrayList<Stat> stats = new ArrayList<>();

        Stat ageStat = new Stat("用户年龄占比", getAgeOptions(searchResult));
        Stat genderStat = new Stat("用户性别占比", getGenderOptions(searchResult));

        stats.add(ageStat);
        stats.add(genderStat);

        return  stats;

    }

    public  List<Option> getAgeOptions(SearchResult searchResult){

        ArrayList<Option> options = new ArrayList<>();

        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation ageCount = aggregations.getTermsAggregation("ageCount");

        //声明每个年龄段的人数
        int age_lt20 = 0;
        int age_20to30 = 0;
        int age_gt30 = 0;

        List<TermsAggregation.Entry> buckets = ageCount.getBuckets();

        for (TermsAggregation.Entry bucket : buckets) {

            if ( Integer.parseInt(bucket.getKey()) < 20){
                age_lt20 += bucket.getCount();
            }else if(Integer.parseInt(bucket.getKey()) >= 30){
                age_gt30 += bucket.getCount();
            }else {
                age_20to30 += bucket.getCount();
            }

        }



        //计算比例
        double sumNum=age_20to30 + age_gt30 + age_lt20;

        DecimalFormat decimalFormat = new DecimalFormat("###.00");

        options.add( new Option("20岁以下", Double.parseDouble(decimalFormat.format(age_lt20 / sumNum * 100))));
        options.add( new Option("20岁到30岁", Double.parseDouble(decimalFormat.format(age_20to30 / sumNum * 100))));
        options.add( new Option("30岁及30岁以上", Double.parseDouble(decimalFormat.format(age_gt30 / sumNum * 100))));

        return options;
    }

    public  List<Option> getGenderOptions(SearchResult searchResult){

        ArrayList<Option> options = new ArrayList<>();

        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation ageCount = aggregations.getTermsAggregation("genderCount");

        int maleCount = 0;
        int femaleCount = 0;

        List<TermsAggregation.Entry> buckets = ageCount.getBuckets();

        for (TermsAggregation.Entry bucket : buckets) {
            if (bucket.getKey().equals("M")){
                maleCount += bucket.getCount();
            }else{
                femaleCount += bucket.getCount();
            }
        }

        double sumCount=maleCount + femaleCount;

        DecimalFormat decimalFormat = new DecimalFormat("###.00");

        double malePercent = Double.parseDouble(decimalFormat.format(maleCount / sumCount * 100));
        double femalePercent = 100-malePercent;

        options.add( new Option("男", malePercent ));
        options.add( new Option("女", femalePercent ));

        return options;

    }

    public List<SaleDetail>  getDetails(SearchResult searchResult){

        ArrayList<SaleDetail> saleDetails = new ArrayList<>();

        List<SearchResult.Hit<SaleDetail, Void>> hits = searchResult.getHits(SaleDetail.class);

        for (SearchResult.Hit<SaleDetail, Void> hit : hits) {
            SaleDetail saleDetail = hit.source;
            saleDetail.setEs_metadata_id(hit.id);

            saleDetails.add(saleDetail);
        }

        return saleDetails;

    }

    /*
            date: 从指定的index中查询
                        gmall2020_sale_detail+ date

             startpage：

             from ：   (startpage -1 ) * size
             size： 5

             keyword: match的关键字

             发送DSL语句到ES，返回结果
     */
    public SearchResult queryES(String date, int startpage, int size, String keyword) throws IOException {


        int from=(startpage - 1 ) * size;

        String indexName= "gmall2020_sale_detail" + date;

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword);

        TermsBuilder aggregationBuilder1 = AggregationBuilders
                .terms("genderCount")
                .field("user_gender")
                .size(10);

        TermsBuilder aggregationBuilder2 = AggregationBuilders
                .terms("ageCount")
                .field("user_age")
                .size(100);


        //原先：String query=xxx  现在： 工具类，将之前构造的各种对象，生成一个查询的字符串
        String query = new SearchSourceBuilder()
                .query(matchQueryBuilder)
                .aggregation(aggregationBuilder1)
                .aggregation(aggregationBuilder2)
                .from(from)
                .size(size)
                .toString();

        // 构造Search  发到到哪个index
        Search search = new Search.Builder(query).addIndex(indexName).build();

        //得到查询结果
        SearchResult searchResult = jestClient.execute(search);

        return searchResult;
    }



    @Override
    public JSONObject getOrderDetail(String date, int startpage, int size, String keyword) throws IOException {

        JSONObject jsonObject = new JSONObject();

        SearchResult searchResult = queryES(date, startpage, size, keyword);

        jsonObject.put("total",searchResult.getTotal());
        jsonObject.put("stat",getStat(searchResult));
        jsonObject.put("detail",getDetails(searchResult));

        return jsonObject;
    }
}
