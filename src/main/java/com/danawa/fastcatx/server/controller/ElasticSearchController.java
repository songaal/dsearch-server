package com.danawa.fastcatx.server.controller;

import com.google.gson.Gson;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.Map;

@RestController
@RequestMapping("/elasticsearch")
@ConfigurationProperties(prefix="elasticsearch")
public class ElasticSearchController {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchController.class);

    private final String SWAGGER = "swagger.json";

    @Value("${elasticsearch.master}")
    private String master;

    @Value("${elasticsearch.protocol}")
    private String protocol;

    private static RestTemplate restTemplate = new RestTemplate();

    @GetMapping("/swagger.json")
    public ResponseEntity<?> swagger() {
        Gson gson = new Gson();
        FileReader fileReader = null;
        ClassPathResource classPathResource = new ClassPathResource(SWAGGER);

        try {
            fileReader = new FileReader(classPathResource.getFile());
            Map<String, Object> swagger = gson.fromJson(fileReader, Map.class);
            return new ResponseEntity<>(swagger, HttpStatus.OK);
        } catch (IOException e) {
            logger.error("", e);
            return new ResponseEntity<>(e, HttpStatus.INTERNAL_SERVER_ERROR);
        } finally {
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e) {
                    logger.error("", fileReader);
                }
            }
        }
    }

    @GetMapping("/{uri}")
    public ResponseEntity<byte[]> proxy(@PathVariable String uri,
                                        HttpServletRequest request,
                                        @RequestBody(required = false) byte[] body) throws URISyntaxException {

        Enumeration<String> headerNames = request.getHeaderNames();
        MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            String headerValue = request.getHeader(headerName);
            headers.add(headerName, headerValue);
        }

        HttpEntity<byte[]> httpEntity = new HttpEntity<>(body, headers);

        String originQueryString = request.getQueryString();
        String url = String.format("%s://%s/%s%s", protocol, master, uri, (StringUtils.isEmpty(originQueryString) ? "" : "?" + originQueryString));

        return restTemplate.exchange(new URI(url), HttpMethod.valueOf(request.getMethod()), httpEntity, byte[].class);
    }


}
