package cn.acyou.scorpio.es;

import cn.acyou.scorpio.es.model.User;
import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class ScorpioEsApplicationTests {

    @Autowired
    @Qualifier("client")
    private RestHighLevelClient client;

    //1. 创建索引请求
    @Test
    void testCreateIndex() throws IOException {
        CreateIndexRequest es = new CreateIndexRequest("es-boot");
        CreateIndexResponse createIndexResponse = client.indices().create(es, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }
    //2. 判断索引库是够存在
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest es = new GetIndexRequest("es-boot");
        boolean exists = client.indices().exists(es, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    //3. 删除索引库
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest es = new DeleteIndexRequest("es-boot");
        AcknowledgedResponse delete = client.indices().delete(es, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    //4. 添加文档
    @Test
    void testAddDocument() throws IOException {
        User user = new User("王二小", 18);
        IndexRequest indexRequest = new IndexRequest("es-boot");
        indexRequest.id("1");
        indexRequest.timeout(TimeValue.timeValueSeconds(60));
        indexRequest.timeout("60s");

        IndexRequest source = indexRequest.source(JSON.toJSONString(user), XContentType.JSON);
        IndexResponse index = client.index(source, RequestOptions.DEFAULT);
        System.out.println(index.toString());
        System.out.println(index.status());
    }

    //5. 查询文档是否存在
    @Test
    void testIsExistDocument() throws IOException {
        GetRequest getRequest = new GetRequest("es-boot", "1");
        //不获取返回的 _source 的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //6. 获取文档内容
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("es-boot", "1");
        //不获取返回的 _source 的上下文了
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString());
        System.out.println(getResponse);
    }
    //7. 更新文档
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("es-boot", "1");
        updateRequest.timeout("10s");
        User user = new User("王二小嘛", 18);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse update = client.update(updateRequest, RequestOptions.DEFAULT);

        System.out.println(update.status());
        System.out.println(update);
    }
    //8. 删除文档
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("es-boot", "1");
        deleteRequest.timeout("10s");
        DeleteResponse delete = client.delete(deleteRequest, RequestOptions.DEFAULT);

        System.out.println(delete.status());
        System.out.println(delete);
    }

    //9. 批量处理
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        List<User> userList = new ArrayList<>();
        userList.add(new User("xxx", 23));
        userList.add(new User("ddd", 24));
        userList.add(new User("fff", 25));
        userList.add(new User("ggg", 26));
        int idx = 1;
        for (User user : userList) {
            //批量修改或者删除
            bulkRequest.add(new IndexRequest("es-boot")
                    .id("" + idx)//不放ID会生成随机ID
                    .source(JSON.toJSONString(user), XContentType.JSON));
            idx ++;
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures());
        System.out.println(bulk.status());
    }

    //10. 查询
    @Test
    void testSearch() throws IOException {
        //搜索请求
        SearchRequest searchRequest = new SearchRequest("es-boot");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构建查询条件
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "xxx");
        //QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(termQueryBuilder);
        searchSourceBuilder.timeout(TimeValue.timeValueSeconds(20));

        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        System.out.println(JSON.toJSONString(hits));
        for (SearchHit hit : hits.getHits()) {
            System.out.println(JSON.toJSONString(hit));
        }
    }
















}
