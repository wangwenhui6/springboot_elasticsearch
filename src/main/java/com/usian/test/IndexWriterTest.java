package com.usian.test;

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.usian.ElasticsearchApp;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {ElasticsearchApp.class})
public class IndexWriterTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    private SearchRequest searchRequest;

    private SearchResponse searchResponse;

    //删除索引库
    @Test
    public void testDeleteIndex() throws IOException {
        //创建“删除索引请求”对象
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("java1906");
        //创建索引客户端
        IndicesClient indices = restHighLevelClient.indices();
        //创建响应对象
        DeleteIndexResponse deleteIndexResponse = indices.delete(deleteIndexRequest, RequestOptions.DEFAULT);
        //得到响应结果
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    //创建索引库
    @Test
    public void testCreateIndex() throws IOException {
        //创建“创建索引请求”对象，并设置索引名称
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("java1906");
        //设置索引参数
        createIndexRequest.settings(Settings.builder().put("number_of_shards",1).
                put("number_of_replicas",0));
        createIndexRequest.mapping("course",
                "{\n" +
                " \t\"properties\": {\n" +
                "      \"name\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"analyzer\":\"ik_max_word\",\n" +
                "          \"search_analyzer\":\"ik_smart\"\n" +
                "      },\n" +
                "      \"description\": {\n" +
                "          \"type\": \"text\",\n" +
                "          \"analyzer\":\"ik_max_word\",\n" +
                "          \"search_analyzer\":\"ik_smart\"\n" +
                "       },\n" +
                "       \"studymodel\": {\n" +
                "          \"type\": \"keyword\"\n" +
                "       },\n" +
                "       \"price\": {\n" +
                "          \"type\": \"float\"\n" +
                "       },\n" +
                "       \"pic\":{\n" +
                "\t\t   \"type\":\"text\",\n" +
                "\t\t   \"index\":false\n" +
                "\t    },\n" +
                "       \"timestamp\": {\n" +
                "      \t\t\"type\":   \"date\",\n" +
                "      \t\t\"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\n" +
                "    \t }\n" +
                "  }\n" +
                "}", XContentType.JSON);
        //创建索引操作客户端
        IndicesClient indices = restHighLevelClient.indices();

        //创建响应对象
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);
        //得到响应结果
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    //添加文档
    @Test
    public void testAddDocument() throws IOException {
        //创建索引对象
        IndexRequest indexRequest = new IndexRequest("java1906", "course", "1");
        indexRequest.source("{\n"+
                " \"name\":\"spring cloud实战\",\n" +
                " \"description\":\"本课程主要从四个章节进行讲解： 1.微服务架构入门 2.spring cloud 基础入门 3.实战Spring Boot 4.注册中心eureka。\",\n" +
                " \"studymodel\":\"201001\",\n" +
                " \"price\":5.6\n" +
                "}",XContentType.JSON);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());

    }

    //批量添加文档
    @Test
    public void testBulkAddDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("java1906","cource").source("{\"name\":\"php实战\",\"description\":\"php谁都不服\",\"studymodel\":\"201001\",\"price\":\"5.6\"}",XContentType.JSON));
        bulkRequest.add(new IndexRequest("java1906","cource").source("{\"name\":\"net实战\",\"description\":\"net从入门到放弃\",\"studymodel\":\"201001\",\"price\":\"7.6\"}",XContentType.JSON));
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.hasFailures());
    }

    //修改文档
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("java1906", "course", "1");
        updateRequest.doc("{\n" +
                " \"price\":6.66\n" +
                "}",XContentType.JSON);
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        DocWriteResponse.Result result = updateResponse.getResult();
        System.out.println(result);
    }

    //删除文档
    @Test
    public void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("java1906", "course", "1");
        DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.getResult());
    }

    //查询文档
    @Test
    public void getDoc() throws IOException {
        GetRequest getRequest = new GetRequest("java1906", "course", "1");
        GetResponse getResponse = restHighLevelClient.get(getRequest);
        boolean exists = getResponse.isExists();
        System.out.println(exists);
        String source = getResponse.getSourceAsString();
        System.out.println(source);
    }

    @Before
    public void initSearchRequest() {
        //搜索请求对象
        searchRequest = new SearchRequest();
        searchRequest.types();
    }

    //搜索typ下的全部记录
    @Test
    public void testSearchAll() throws IOException {
        //搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

    }

    @After
    public void displayDoc() {
        //搜索匹配结果
        SearchHits hits = searchResponse.getHits();
        //搜索总记录数
        long totalHits = hits.totalHits;
        System.out.println("共搜索到"+totalHits+"条文档");
        //匹配的文档
        SearchHit[] searchHits = hits.getHits();
        // 日期格式化对象
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (SearchHit hit : searchHits) {
            // 文档id
            String id = hit.getId();
            System.out.println("id：" + id);
            // 源文档内容
            String source = hit.getSourceAsString();
            System.out.println(source);
        }
    }

    //分页查询
    @Test
    public void testSearchPage() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(1);
        searchSourceBuilder.size(2);
        searchSourceBuilder.sort("price", SortOrder.ASC);
        //设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(this.searchRequest, RequestOptions.DEFAULT);
    }

    //match查询(operator：or 表示 只要有一个词在文档中出现则就符合条件，and表示每个词都在文档中出现则才符合条件。)
    @Test
    public void testMathQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name","spring开发").operator(Operator.AND));
        //设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //multi_match查询(matchQuery是在一个field中去匹配，multiQuery是拿关键字去多个Field中匹配。)
    @Test
    public void testMutlMatchQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("开发","name","decription"));
        //设置搜索源
        searchRequest.source(searchSourceBuilder);
        //执行搜索
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //bool查询
    @Test
    public void testBooleanQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //Bool
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //must
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.must(QueryBuilders.matchQuery("description","开发"));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    //filter查询
    @Test
    public void testFilterQuery() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery("name","开发"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(10).lte(100));
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    }
}
