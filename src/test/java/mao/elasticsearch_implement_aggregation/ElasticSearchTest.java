package mao.elasticsearch_implement_aggregation;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;


/**
 * Project name(项目名称)：elasticsearch_Implement_aggregation
 * Package(包名): mao.elasticsearch_implement_aggregation
 * Class(类名): ElasticSearchTest
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/5/29
 * Time(创建时间)： 21:03
 * Version(版本): 1.0
 * Description(描述)： SpringBootTest
 * <p>
 * <p>
 * 电视案例
 * 索引：
 * <pre>
 *
 * {
 *   "tvs" : {
 *     "aliases" : { },
 *     "mappings" : {
 *       "properties" : {
 *         "brand" : {
 *           "type" : "keyword"
 *         },
 *         "color" : {
 *           "type" : "keyword"
 *         },
 *         "price" : {
 *           "type" : "long"
 *         },
 *         "sold_date" : {
 *           "type" : "date"
 *         }
 *       }
 *     },
 *     "settings" : {
 *       "index" : {
 *         "routing" : {
 *           "allocation" : {
 *             "include" : {
 *               "_tier_preference" : "data_content"
 *             }
 *           }
 *         },
 *         "number_of_shards" : "1",
 *         "provided_name" : "tvs",
 *         "creation_date" : "1653799931947",
 *         "number_of_replicas" : "1",
 *         "uuid" : "UPq3NuVHTlWq1r9GrCj4fw",
 *         "version" : {
 *           "created" : "8010399"
 *         }
 *       }
 *     }
 *   }
 * }
 *
 * </pre>
 * <p>
 * 数据：
 * <pre>
 *
 * {
 *   "took" : 0,
 *   "timed_out" : false,
 *   "_shards" : {
 *     "total" : 1,
 *     "successful" : 1,
 *     "skipped" : 0,
 *     "failed" : 0
 *   },
 *   "hits" : {
 *     "total" : {
 *       "value" : 14,
 *       "relation" : "eq"
 *     },
 *     "max_score" : 1.0,
 *     "hits" : [
 *       {
 *         "_index" : "tvs",
 *         "_id" : "66ouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 1000,
 *           "color" : "红色",
 *           "brand" : "长虹",
 *           "sold_date" : "2019-10-28"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "7KouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 2000,
 *           "color" : "红色",
 *           "brand" : "长虹",
 *           "sold_date" : "2019-11-05"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "7aouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 3000,
 *           "color" : "绿色",
 *           "brand" : "小米",
 *           "sold_date" : "2019-05-18"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "7qouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 1500,
 *           "color" : "蓝色",
 *           "brand" : "TCL",
 *           "sold_date" : "2019-07-02"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "76ouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 1200,
 *           "color" : "绿色",
 *           "brand" : "TCL",
 *           "sold_date" : "2019-08-19"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "8KouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 2000,
 *           "color" : "红色",
 *           "brand" : "长虹",
 *           "sold_date" : "2019-11-05"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "8aouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 8000,
 *           "color" : "红色",
 *           "brand" : "三星",
 *           "sold_date" : "2020-01-01"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "8qouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 2500,
 *           "color" : "蓝色",
 *           "brand" : "小米",
 *           "sold_date" : "2020-02-12"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "86ouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 4500,
 *           "color" : "绿色",
 *           "brand" : "小米",
 *           "sold_date" : "2020-04-22"
 *         }
 *       },
 *       {
 *         "_index" : "tvs",
 *         "_id" : "9KouDoEBEpQthbP41cfj",
 *         "_score" : 1.0,
 *         "_source" : {
 *           "price" : 6100,
 *           "color" : "蓝色",
 *           "brand" : "三星",
 *           "sold_date" : "2020-05-16"
 *         }
 *       }
 *     ]
 *   }
 * }
 *
 * </pre>
 */

@SpringBootTest
public class ElasticSearchTest
{

    private static RestHighLevelClient client;

    /**
     * Before all.
     */
    @BeforeAll
    static void beforeAll()
    {
        client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ));
    }

    /**
     * After all.
     *
     * @throws IOException the io exception
     */
    @AfterAll
    static void afterAll() throws IOException
    {
        client.close();
    }

    /**
     * Ping.
     *
     * @throws IOException the io exception
     */
    @Test
    void ping() throws IOException
    {
        boolean ping = client.ping(RequestOptions.DEFAULT);
        System.out.println(ping);
    }


    /**
     * 统计哪种颜色的电视销量最高
     * <p>
     * 请求内容：
     * <pre>
     *
     * GET /tvs/_search
     * {
     *   "query":
     *   {
     *     "match_all": {}
     *   },
     *   "size": 0,
     *   "aggs":
     *   {
     *     "popular_colors":
     *     {
     *       "terms":
     *       {
     *         "field": "color"
     *       }
     *     }
     *   }
     * }
     *
     * </pre>
     * <p>
     * 结果：
     * <pre>
     *
     * {
     *   "took" : 12,
     *   "timed_out" : false,
     *   "_shards" : {
     *     "total" : 1,
     *     "successful" : 1,
     *     "skipped" : 0,
     *     "failed" : 0
     *   },
     *   "hits" : {
     *     "total" : {
     *       "value" : 14,
     *       "relation" : "eq"
     *     },
     *     "max_score" : null,
     *     "hits" : [ ]
     *   },
     *   "aggregations" : {
     *     "popular_colors" : {
     *       "doc_count_error_upper_bound" : 0,
     *       "sum_other_doc_count" : 0,
     *       "buckets" : [
     *         {
     *           "key" : "红色",
     *           "doc_count" : 5
     *         },
     *         {
     *           "key" : "蓝色",
     *           "doc_count" : 4
     *         },
     *         {
     *           "key" : "绿色",
     *           "doc_count" : 3
     *         },
     *         {
     *           "key" : "白色",
     *           "doc_count" : 1
     *         },
     *         {
     *           "key" : "黑色",
     *           "doc_count" : 1
     *         }
     *       ]
     *     }
     *   }
     * }
     *
     * </pre>
     * <p>
     * 程序结果：
     * <pre>
     *
     * ----key：红色
     * ----doc_count：5
     * ----------------------------------------
     * ----key：蓝色
     * ----doc_count：4
     * ----------------------------------------
     * ----key：绿色
     * ----doc_count：3
     * ----------------------------------------
     * ----key：白色
     * ----doc_count：1
     * ----------------------------------------
     * ----key：黑色
     * ----doc_count：1
     * ----------------------------------------
     *
     * </pre>
     *
     * @throws Exception Exception
     */
    @Test
    void aggregation1() throws Exception
    {
        //构建请求
        SearchRequest searchRequest = new SearchRequest("tvs");
        //构建请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页
        searchSourceBuilder.size(0);
        //聚合
        searchSourceBuilder.aggregation(AggregationBuilders.terms("popular_colors").field("color"));
        //放入到请求中
        searchRequest.source(searchSourceBuilder);
        //发起请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取数据
        //获取aggregations部分
        Aggregations aggregations = searchResponse.getAggregations();
        //获得popular_colors
        Terms popular_colors = aggregations.get("popular_colors");
        //获取buckets部分
        List<? extends Terms.Bucket> buckets = popular_colors.getBuckets();
        //遍历
        for (Terms.Bucket bucket : buckets)
        {
            //获取数据
            String key = (String) bucket.getKey();
            long docCount = bucket.getDocCount();
            //打印
            System.out.println("----key：" + key);
            System.out.println("----doc_count：" + docCount);
            System.out.println("----------------------------------------");
        }
    }


    /**
     * 统计每种颜色电视平均价格
     * <p>
     * 请求内容：
     * <pre>
     *
     * GET /tvs/_search
     * {
     *   "query":
     *   {
     *     "match_all": {}
     *   },
     *   "size": 0,
     *   "aggs":
     *   {
     *     "group_by_colors":
     *     {
     *       "terms":
     *       {
     *         "field": "color"
     *       },
     *       "aggs": {
     *         "avg_price":
     *         {
     *           "avg":
     *           {
     *             "field": "price"
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     *
     *
     *
     * </pre>
     * <p>
     * 结果：
     * <pre>
     *
     * {
     *   "took" : 1,
     *   "timed_out" : false,
     *   "_shards" : {
     *     "total" : 1,
     *     "successful" : 1,
     *     "skipped" : 0,
     *     "failed" : 0
     *   },
     *   "hits" : {
     *     "total" : {
     *       "value" : 14,
     *       "relation" : "eq"
     *     },
     *     "max_score" : null,
     *     "hits" : [ ]
     *   },
     *   "aggregations" : {
     *     "group_by_colors" : {
     *       "doc_count_error_upper_bound" : 0,
     *       "sum_other_doc_count" : 0,
     *       "buckets" : [
     *         {
     *           "key" : "红色",
     *           "doc_count" : 5,
     *           "avg_price" : {
     *             "value" : 4300.0
     *           }
     *         },
     *         {
     *           "key" : "蓝色",
     *           "doc_count" : 4,
     *           "avg_price" : {
     *             "value" : 3575.0
     *           }
     *         },
     *         {
     *           "key" : "绿色",
     *           "doc_count" : 3,
     *           "avg_price" : {
     *             "value" : 2900.0
     *           }
     *         },
     *         {
     *           "key" : "白色",
     *           "doc_count" : 1,
     *           "avg_price" : {
     *             "value" : 2100.0
     *           }
     *         },
     *         {
     *           "key" : "黑色",
     *           "doc_count" : 1,
     *           "avg_price" : {
     *             "value" : 4800.0
     *           }
     *         }
     *       ]
     *     }
     *   }
     * }
     *
     * </pre>
     * <p>
     * 程序结果：
     * <pre>
     *
     * ----key：红色
     * ----doc_count：5
     * ----平均价格：4300.0
     * ----------------------------------------
     * ----key：蓝色
     * ----doc_count：4
     * ----平均价格：3575.0
     * ----------------------------------------
     * ----key：绿色
     * ----doc_count：3
     * ----平均价格：2900.0
     * ----------------------------------------
     * ----key：白色
     * ----doc_count：1
     * ----平均价格：2100.0
     * ----------------------------------------
     * ----key：黑色
     * ----doc_count：1
     * ----平均价格：4800.0
     * ----------------------------------------
     *
     * </pre>
     *
     * @throws Exception Exception
     */
    @Test
    void aggregation2() throws Exception
    {
        //构建请求
        SearchRequest searchRequest = new SearchRequest("tvs");
        //构建请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页
        searchSourceBuilder.size(0);
        //聚合
        searchSourceBuilder.aggregation(AggregationBuilders.terms("group_by_colors").field("color")
                .subAggregation(AggregationBuilders.avg("avg_price").field("price")));
        //放入到请求中
        searchRequest.source(searchSourceBuilder);
        //发起请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取数据
        //获取aggregations部分
        Aggregations aggregations = searchResponse.getAggregations();
        //获得group_by_colors
        Terms group_by_colors = aggregations.get("group_by_colors");
        //获取buckets部分
        List<? extends Terms.Bucket> buckets = group_by_colors.getBuckets();
        //遍历
        for (Terms.Bucket bucket : buckets)
        {
            //获取数据
            String key = (String) bucket.getKey();
            long docCount = bucket.getDocCount();
            Avg avg_price = bucket.getAggregations().get("avg_price");
            double avgPriceValue = avg_price.getValue();
            //打印
            System.out.println("----key：" + key);
            System.out.println("----doc_count：" + docCount);
            System.out.println("----平均价格：" + avgPriceValue);
            System.out.println("----------------------------------------");
        }
    }


    /**
     * 统计每个颜色下，平均价格及每个颜色下，每个品牌的平均价格
     * <p>
     * 请求内容：
     * <pre>
     *
     * GET /tvs/_search
     * {
     *   "query":
     *   {
     *     "match_all": {}
     *   },
     *   "size": 0,
     *   "aggs":
     *   {
     *     "group_by_colors":
     *     {
     *       "terms":
     *       {
     *         "field": "color"
     *       },
     *       "aggs":
     *       {
     *         "avg_price":
     *         {
     *           "avg":
     *           {
     *             "field": "price"
     *           }
     *         },
     *         "group_by_brand":
     *         {
     *           "terms":
     *           {
     *             "field": "brand"
     *           },
     *           "aggs":
     *           {
     *             "avg_price":
     *             {
     *               "avg":
     *               {
     *                 "field": "price"
     *               }
     *             }
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     *
     * </pre>
     * <p>
     * 结果：
     * <pre>
     *
     * {
     *   "took" : 1,
     *   "timed_out" : false,
     *   "_shards" : {
     *     "total" : 1,
     *     "successful" : 1,
     *     "skipped" : 0,
     *     "failed" : 0
     *   },
     *   "hits" : {
     *     "total" : {
     *       "value" : 14,
     *       "relation" : "eq"
     *     },
     *     "max_score" : null,
     *     "hits" : [ ]
     *   },
     *   "aggregations" : {
     *     "group_by_colors" : {
     *       "doc_count_error_upper_bound" : 0,
     *       "sum_other_doc_count" : 0,
     *       "buckets" : [
     *         {
     *           "key" : "红色",
     *           "doc_count" : 5,
     *           "avg_price" : {
     *             "value" : 4300.0
     *           },
     *           "group_by_brand" : {
     *             "doc_count_error_upper_bound" : 0,
     *             "sum_other_doc_count" : 0,
     *             "buckets" : [
     *               {
     *                 "key" : "长虹",
     *                 "doc_count" : 3,
     *                 "avg_price" : {
     *                   "value" : 1666.6666666666667
     *                 }
     *               },
     *               {
     *                 "key" : "三星",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 8000.0
     *                 }
     *               },
     *               {
     *                 "key" : "小米",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 8500.0
     *                 }
     *               }
     *             ]
     *           }
     *         },
     *         {
     *           "key" : "蓝色",
     *           "doc_count" : 4,
     *           "avg_price" : {
     *             "value" : 3575.0
     *           },
     *           "group_by_brand" : {
     *             "doc_count_error_upper_bound" : 0,
     *             "sum_other_doc_count" : 0,
     *             "buckets" : [
     *               {
     *                 "key" : "TCL",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 1500.0
     *                 }
     *               },
     *               {
     *                 "key" : "三星",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 6100.0
     *                 }
     *               },
     *               {
     *                 "key" : "小米",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 2500.0
     *                 }
     *               },
     *               {
     *                 "key" : "长虹",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 4200.0
     *                 }
     *               }
     *             ]
     *           }
     *         },
     *         {
     *           "key" : "绿色",
     *           "doc_count" : 3,
     *           "avg_price" : {
     *             "value" : 2900.0
     *           },
     *           "group_by_brand" : {
     *             "doc_count_error_upper_bound" : 0,
     *             "sum_other_doc_count" : 0,
     *             "buckets" : [
     *               {
     *                 "key" : "小米",
     *                 "doc_count" : 2,
     *                 "avg_price" : {
     *                   "value" : 3750.0
     *                 }
     *               },
     *               {
     *                 "key" : "TCL",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 1200.0
     *                 }
     *               }
     *             ]
     *           }
     *         },
     *         {
     *           "key" : "白色",
     *           "doc_count" : 1,
     *           "avg_price" : {
     *             "value" : 2100.0
     *           },
     *           "group_by_brand" : {
     *             "doc_count_error_upper_bound" : 0,
     *             "sum_other_doc_count" : 0,
     *             "buckets" : [
     *               {
     *                 "key" : "TCL",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 2100.0
     *                 }
     *               }
     *             ]
     *           }
     *         },
     *         {
     *           "key" : "黑色",
     *           "doc_count" : 1,
     *           "avg_price" : {
     *             "value" : 4800.0
     *           },
     *           "group_by_brand" : {
     *             "doc_count_error_upper_bound" : 0,
     *             "sum_other_doc_count" : 0,
     *             "buckets" : [
     *               {
     *                 "key" : "小米",
     *                 "doc_count" : 1,
     *                 "avg_price" : {
     *                   "value" : 4800.0
     *                 }
     *               }
     *             ]
     *           }
     *         }
     *       ]
     *     }
     *   }
     * }
     *
     * </pre>
     * <p>
     * 程序结果：
     * <pre>
     *
     * ----key：红色
     * ----doc_count：5
     * ----平均价格：4300.0
     * ----group_by_brand：
     * --------key：长虹
     * --------doc_count：3
     * --------平均价格：1666.6666666666667
     * ----------------
     * --------key：三星
     * --------doc_count：1
     * --------平均价格：8000.0
     * ----------------
     * --------key：小米
     * --------doc_count：1
     * --------平均价格：8500.0
     * ----------------
     * ----------------------------------------
     * ----key：蓝色
     * ----doc_count：4
     * ----平均价格：3575.0
     * ----group_by_brand：
     * --------key：TCL
     * --------doc_count：1
     * --------平均价格：1500.0
     * ----------------
     * --------key：三星
     * --------doc_count：1
     * --------平均价格：6100.0
     * ----------------
     * --------key：小米
     * --------doc_count：1
     * --------平均价格：2500.0
     * ----------------
     * --------key：长虹
     * --------doc_count：1
     * --------平均价格：4200.0
     * ----------------
     * ----------------------------------------
     * ----key：绿色
     * ----doc_count：3
     * ----平均价格：2900.0
     * ----group_by_brand：
     * --------key：小米
     * --------doc_count：2
     * --------平均价格：3750.0
     * ----------------
     * --------key：TCL
     * --------doc_count：1
     * --------平均价格：1200.0
     * ----------------
     * ----------------------------------------
     * ----key：白色
     * ----doc_count：1
     * ----平均价格：2100.0
     * ----group_by_brand：
     * --------key：TCL
     * --------doc_count：1
     * --------平均价格：2100.0
     * ----------------
     * ----------------------------------------
     * ----key：黑色
     * ----doc_count：1
     * ----平均价格：4800.0
     * ----group_by_brand：
     * --------key：小米
     * --------doc_count：1
     * --------平均价格：4800.0
     * ----------------
     * ----------------------------------------
     *
     * </pre>
     *
     * @throws Exception Exception
     */
    @Test
    void aggregation3() throws Exception
    {
        //构建请求
        SearchRequest searchRequest = new SearchRequest("tvs");
        //构建请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页
        searchSourceBuilder.size(0);
        //聚合
        searchSourceBuilder.aggregation(
                AggregationBuilders.terms("group_by_colors").field("color")
                        .subAggregations(AggregatorFactories.builder()
                                .addAggregator(AggregationBuilders.avg("avg_price").field("price"))
                                .addAggregator(AggregationBuilders.terms("group_by_brand").field("brand")
                                        .subAggregation(AggregationBuilders.avg("avg_price").field("price")))));
        //放入到请求中
        searchRequest.source(searchSourceBuilder);
        //发起请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取数据
        //获取aggregations部分
        Aggregations aggregations = searchResponse.getAggregations();
        //获得group_by_colors
        Terms group_by_colors = aggregations.get("group_by_colors");
        //获取buckets部分
        List<? extends Terms.Bucket> buckets = group_by_colors.getBuckets();
        //遍历
        for (Terms.Bucket bucket : buckets)
        {
            //获取数据
            String key = (String) bucket.getKey();
            long docCount = bucket.getDocCount();
            Avg avg_price = bucket.getAggregations().get("avg_price");
            Terms group_by_brand = bucket.getAggregations().get("group_by_brand");
            List<? extends Terms.Bucket> buckets1 = group_by_brand.getBuckets();
            double avgPriceValue = avg_price.getValue();
            //打印
            System.out.println("----key：" + key);
            System.out.println("----doc_count：" + docCount);
            System.out.println("----平均价格：" + avgPriceValue);
            System.out.println("----group_by_brand：");
            for (Terms.Bucket bucket1 : buckets1)
            {
                //获取数据
                String key1 = (String) bucket1.getKey();
                long docCount1 = bucket1.getDocCount();
                Avg avg_price1 = bucket1.getAggregations().get("avg_price");
                double avgPrice1Value = avg_price1.getValue();
                //打印
                System.out.println("--------key：" + key1);
                System.out.println("--------doc_count：" + docCount1);
                System.out.println("--------平均价格：" + avgPrice1Value);
                System.out.println("----------------");
            }
            System.out.println("----------------------------------------");
        }
    }


    /**
     * 求每个颜色的最大价格、最小价格、平均价格和总价格
     * 请求内容：
     * <pre>
     *
     * GET /tvs/_search
     * {
     *   "query":
     *   {
     *     "match_all": {}
     *   },
     *   "size": 0,
     *   "aggs":
     *   {
     *     "group_by_color":
     *     {
     *       "terms":
     *       {
     *         "field": "color"
     *       },
     *       "aggs":
     *       {
     *         "max_price":
     *         {
     *           "max":
     *           {
     *             "field": "price"
     *           }
     *         },
     *         "min_price":
     *         {
     *           "min":
     *           {
     *             "field": "price"
     *           }
     *         },
     *         "avg_price":
     *         {
     *           "avg":
     *           {
     *             "field": "price"
     *           }
     *         },
     *         "sum_price":
     *         {
     *           "sum":
     *           {
     *             "field": "price"
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     *
     * </pre>
     * <p>
     * 结果：
     * <pre>
     *
     * {
     *   "took" : 1,
     *   "timed_out" : false,
     *   "_shards" : {
     *     "total" : 1,
     *     "successful" : 1,
     *     "skipped" : 0,
     *     "failed" : 0
     *   },
     *   "hits" : {
     *     "total" : {
     *       "value" : 14,
     *       "relation" : "eq"
     *     },
     *     "max_score" : null,
     *     "hits" : [ ]
     *   },
     *   "aggregations" : {
     *     "group_by_color" : {
     *       "doc_count_error_upper_bound" : 0,
     *       "sum_other_doc_count" : 0,
     *       "buckets" : [
     *         {
     *           "key" : "红色",
     *           "doc_count" : 5,
     *           "max_price" : {
     *             "value" : 8500.0
     *           },
     *           "min_price" : {
     *             "value" : 1000.0
     *           },
     *           "avg_price" : {
     *             "value" : 4300.0
     *           },
     *           "sum_price" : {
     *             "value" : 21500.0
     *           }
     *         },
     *         {
     *           "key" : "蓝色",
     *           "doc_count" : 4,
     *           "max_price" : {
     *             "value" : 6100.0
     *           },
     *           "min_price" : {
     *             "value" : 1500.0
     *           },
     *           "avg_price" : {
     *             "value" : 3575.0
     *           },
     *           "sum_price" : {
     *             "value" : 14300.0
     *           }
     *         },
     *         {
     *           "key" : "绿色",
     *           "doc_count" : 3,
     *           "max_price" : {
     *             "value" : 4500.0
     *           },
     *           "min_price" : {
     *             "value" : 1200.0
     *           },
     *           "avg_price" : {
     *             "value" : 2900.0
     *           },
     *           "sum_price" : {
     *             "value" : 8700.0
     *           }
     *         },
     *         {
     *           "key" : "白色",
     *           "doc_count" : 1,
     *           "max_price" : {
     *             "value" : 2100.0
     *           },
     *           "min_price" : {
     *             "value" : 2100.0
     *           },
     *           "avg_price" : {
     *             "value" : 2100.0
     *           },
     *           "sum_price" : {
     *             "value" : 2100.0
     *           }
     *         },
     *         {
     *           "key" : "黑色",
     *           "doc_count" : 1,
     *           "max_price" : {
     *             "value" : 4800.0
     *           },
     *           "min_price" : {
     *             "value" : 4800.0
     *           },
     *           "avg_price" : {
     *             "value" : 4800.0
     *           },
     *           "sum_price" : {
     *             "value" : 4800.0
     *           }
     *         }
     *       ]
     *     }
     *   }
     * }
     *
     * </pre>
     * <p>
     * 程序结果：
     * <pre>
     *
     * ----key：红色
     * ----doc_count：5
     * ----group_by_brand：
     * ----max_price：8500.0
     * ----min_price：1000.0
     * ----avg_price：4300.0
     * ----sum_price：21500.0
     * ----------------------------------------
     * ----key：蓝色
     * ----doc_count：4
     * ----group_by_brand：
     * ----max_price：6100.0
     * ----min_price：1500.0
     * ----avg_price：3575.0
     * ----sum_price：14300.0
     * ----------------------------------------
     * ----key：绿色
     * ----doc_count：3
     * ----group_by_brand：
     * ----max_price：4500.0
     * ----min_price：1200.0
     * ----avg_price：2900.0
     * ----sum_price：8700.0
     * ----------------------------------------
     * ----key：白色
     * ----doc_count：1
     * ----group_by_brand：
     * ----max_price：2100.0
     * ----min_price：2100.0
     * ----avg_price：2100.0
     * ----sum_price：2100.0
     * ----------------------------------------
     * ----key：黑色
     * ----doc_count：1
     * ----group_by_brand：
     * ----max_price：4800.0
     * ----min_price：4800.0
     * ----avg_price：4800.0
     * ----sum_price：4800.0
     * ----------------------------------------
     *
     * </pre>
     *
     * @throws Exception Exception
     */
    @Test
    void aggregation4() throws Exception
    {
        //构建请求
        SearchRequest searchRequest = new SearchRequest("tvs");
        //构建请求体
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //查询
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //分页
        searchSourceBuilder.size(0);
        //聚合
        searchSourceBuilder.aggregation(
                AggregationBuilders.terms("group_by_colors").field("color")
                        .subAggregations(AggregatorFactories.builder()
                                .addAggregator(AggregationBuilders.max("max_price").field("price"))
                                .addAggregator(AggregationBuilders.min("min_price").field("price"))
                                .addAggregator(AggregationBuilders.avg("avg_price").field("price"))
                                .addAggregator(AggregationBuilders.sum("sum_price").field("price")))
        );

        //放入到请求中
        searchRequest.source(searchSourceBuilder);
        //发起请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //获取数据
        //获取aggregations部分
        Aggregations aggregations = searchResponse.getAggregations();
        //获得group_by_colors
        Terms group_by_colors = aggregations.get("group_by_colors");
        //获取buckets部分
        List<? extends Terms.Bucket> buckets = group_by_colors.getBuckets();
        //遍历
        for (Terms.Bucket bucket : buckets)
        {
            //获取数据
            String key = (String) bucket.getKey();
            long docCount = bucket.getDocCount();
            Max max_price = bucket.getAggregations().get("max_price");
            Min min_price = bucket.getAggregations().get("min_price");
            Avg avg_price = bucket.getAggregations().get("avg_price");
            Sum sum_price = bucket.getAggregations().get("sum_price");
            double maxPriceValue = max_price.getValue();
            double minPriceValue = min_price.getValue();
            double avgPriceValue = avg_price.getValue();
            double sumPriceValue = sum_price.getValue();

            //打印
            System.out.println("----key：" + key);
            System.out.println("----doc_count：" + docCount);
            System.out.println("----group_by_brand：");
            System.out.println("----max_price：" + maxPriceValue);
            System.out.println("----min_price：" + minPriceValue);
            System.out.println("----avg_price：" + avgPriceValue);
            System.out.println("----sum_price：" + sumPriceValue);


            System.out.println("----------------------------------------");
        }
    }
}
