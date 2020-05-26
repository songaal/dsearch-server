package com.danawa.fastcatx.server.services;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

@Service
public class ElasticsearchProxyService {
    private static Logger logger = LoggerFactory.getLogger(ElasticsearchProxyService.class);

    private RestHighLevelClient client;

    public ElasticsearchProxyService(@Qualifier("getRestHighLevelClient") RestHighLevelClient client) {
        this.client = client;
    }

    public Response proxy(HttpServletRequest request, Map<String, String> queryStringMap, byte[] body) throws IOException {
//        /elasticsearch  substring 14
        Request req = new Request(request.getMethod(), request.getRequestURI().substring(14));

        if (queryStringMap != null) {
            Iterator<String> keys = queryStringMap.keySet().iterator();
            while (keys.hasNext()) {
                String key = keys.next();
                req.addParameter(key, queryStringMap.get(key));
            }
        }

        if (body != null) {
            req.setEntity(new NStringEntity(new String(body), ContentType.APPLICATION_JSON));
        }

        return  client.getLowLevelClient().performRequest(req);
    }


// 전송방식 변경
//    private String username;
//    private String password;
//    private List<String> nodes;

//    public ProxyService(ElasticSearchConfig elasticSearchConfig) {
//        this.username = elasticSearchConfig.getUsername();
//        this.password = elasticSearchConfig.getPassword();
//        this.nodes = elasticSearchConfig.getNodes();
//    }
//    private RestTemplate restTemplate = new RestTemplate();
//
//    public ResponseEntity<byte[]> proxy(HttpServletRequest request, byte[] body) throws ServiceException {
//        Enumeration<String> headerNames = request.getHeaderNames();
//        MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
//        while (headerNames.hasMoreElements()) {
//            String headerName = headerNames.nextElement();
//            String headerValue = request.getHeader(headerName);
//            headers.add(headerName, headerValue);
//        }
//
//        HttpEntity<byte[]> httpEntity = new HttpEntity<>(body, headers);
//
//        String originQueryString = request.getQueryString();
//        ResponseEntity<byte[]> response = null;
//        for (String node : nodes) {
//            String url = String.format("%s%s%s", node, request.getRequestURI().substring(14), (StringUtils.isEmpty(originQueryString) ? "" : "?" + originQueryString));
//            try {
//                response = restTemplate.exchange(new URI(url), HttpMethod.valueOf(request.getMethod()), httpEntity, byte[].class);
//                break;
//            } catch (RestClientException | URISyntaxException e) {
//                logger.debug("proxy fail : {}, {}", node, e);
//                throw new ServiceException("test");
//            }
//        }
//        return response;
//    }
//
//
//
//
//    public String getUsername() {
//        return this.username;
//    }
//
//    public void setUsername(String username) {
//        this.username = username;
//    }
//
//    public String getPassword() {
//        return this.password;
//    }
//
//    public void setPassword(String password) {
//        this.password = password;
//    }
//
//    public List<String> getNodes() {
//        return nodes;
//    }
//
//    public void setNodes(List<String> nodes) {
//        this.nodes = nodes;
//    }



}
