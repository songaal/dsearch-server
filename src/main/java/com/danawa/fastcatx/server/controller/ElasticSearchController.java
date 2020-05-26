package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.excpetions.ServiceException;
import com.danawa.fastcatx.server.services.ElasticsearchProxyService;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@RestController
@RequestMapping("/elasticsearch")
public class ElasticSearchController {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchController.class);

    private ElasticsearchProxyService proxyService;

    public ElasticSearchController(ElasticsearchProxyService proxyService) {
        this.proxyService = proxyService;
    }

    @GetMapping(value = {"/**/*", "*"})
    @CrossOrigin("*")
    public ResponseEntity<?> proxy(HttpServletRequest request,
                                        @RequestBody(required = false) byte[] body) throws ServiceException, IOException {
        logger.debug("proxy {}", request.getRequestURI());
        Response response = proxyService.proxy(request, body);
        RequestLine requestLine = response.getRequestLine();
        HttpHost host = response.getHost();
        int statusCode = response.getStatusLine().getStatusCode();
        Header[] headers = response.getHeaders();
        String responseBody = EntityUtils.toString(response.getEntity());

        return new ResponseEntity<>(responseBody, HttpStatus.OK);
    }
}
