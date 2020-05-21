package com.danawa.fastcatx.server.controller;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;

@RestController
@RequestMapping("/es")
@ConfigurationProperties(prefix="elasticsearch")
public class ElasticSearchController {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchController.class);

    @Value("${elasticsearch.master}")
    private String master;

    @Value("${elasticsearch.protocol}")
    private String protocol;

    private static RestTemplate restTemplate = new RestTemplate();

    @GetMapping("/**/*")
    public ResponseEntity<byte[]> proxy(HttpServletRequest request,
                                        @RequestBody(required = false) byte[] body) throws URISyntaxException {
        logger.debug("proxy {}", request.getRequestURI());
        Enumeration<String> headerNames = request.getHeaderNames();
        MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            String headerValue = request.getHeader(headerName);
            headers.add(headerName, headerValue);
        }

        HttpEntity<byte[]> httpEntity = new HttpEntity<>(body, headers);

        String originQueryString = request.getQueryString();
        String url = String.format("%s://%s%s%s", protocol, master, request.getRequestURI().substring(14), (StringUtils.isEmpty(originQueryString) ? "" : "?" + originQueryString));

        return restTemplate.exchange(new URI(url), HttpMethod.valueOf(request.getMethod()), httpEntity, byte[].class);
    }


}
