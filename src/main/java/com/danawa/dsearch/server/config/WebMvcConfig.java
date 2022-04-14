package com.danawa.dsearch.server.config;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig {

    @Bean
    public WebMvcConfigurer webMvcConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/*/*")
                        .allowedOriginPatterns("/*/*")
                        .allowedOriginPatterns("/**/*")
                        .allowedOriginPatterns("*")
                        .allowedOrigins("*")
                        .allowedOrigins("/*/*")
                        .allowedOrigins("/**/*")
                        .allowedMethods(
                                HttpMethod.GET.name(),
                                HttpMethod.POST.name(),
                                HttpMethod.PUT.name(),
                                HttpMethod.DELETE.name(),
                                HttpMethod.OPTIONS.name(),
                                HttpMethod.PATCH.name(),
                                HttpMethod.HEAD.name()
                        )
                        .allowCredentials(true)
                        .exposedHeaders("x-bearer-token")
                        .maxAge(3600);

                registry.addMapping("/**")
                        .allowedOriginPatterns("*")
                        .allowedMethods(
                                HttpMethod.GET.name(),
                                HttpMethod.POST.name(),
                                HttpMethod.PUT.name(),
                                HttpMethod.DELETE.name(),
                                HttpMethod.OPTIONS.name(),
                                HttpMethod.PATCH.name(),
                                HttpMethod.HEAD.name()
                                )
                        .allowCredentials(true)
                        .exposedHeaders("x-bearer-token")
                        .maxAge(3600);
            }
        };
    }


}
