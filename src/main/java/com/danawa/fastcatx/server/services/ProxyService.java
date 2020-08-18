package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.excpetions.ServiceException;
import org.apache.http.client.utils.URLEncodedUtils;
import org.springframework.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;


@Service
public class ProxyService {
    private static Logger logger = LoggerFactory.getLogger(ProxyService.class);

    public ProxyService( ) {

    }

    public ResponseEntity<byte[]> proxy(String url, String keyword) throws UnsupportedEncodingException {
        ResponseEntity response= null;
        url += URLEncoder.encode(keyword, "UTF-8");
        HttpEntity httpEntity = new HttpEntity<>(url);
        RestTemplate restTemplate = new RestTemplate();
        try {
            response = restTemplate.exchange(new URI(url), HttpMethod.GET, httpEntity, byte[].class);
        } catch (RestClientException | URISyntaxException e) {
            logger.debug("proxy fail : {}", e);
        }
        return response;
    }

}
