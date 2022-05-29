package mao.elasticsearch_implement_aggregation;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
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
     *
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
}
