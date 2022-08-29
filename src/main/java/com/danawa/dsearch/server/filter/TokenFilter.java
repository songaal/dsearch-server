package com.danawa.dsearch.server.filter;

import com.auth0.jwt.exceptions.JWTVerificationException;
import com.danawa.dsearch.server.utils.JWTUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class TokenFilter extends OncePerRequestFilter {

    private JWTUtils jwtUtils;

    @Autowired
    public TokenFilter(Environment env) {
        String secret = env.getRequiredProperty("dsearch.auth.secret");
        long expirationTimeMillis = Long.parseLong(env.getRequiredProperty("dsearch.auth.expiration-time-millis"));
        this.jwtUtils = new JWTUtils(secret, expirationTimeMillis);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, FilterChain filterChain) throws ServletException, IOException {
        if (AuthFilter.bypassUri.contains(httpServletRequest.getRequestURI())) {
            filterChain.doFilter(httpServletRequest, httpServletResponse);
            return;
        }

        String token = httpServletRequest.getHeader("x-bearer-token");
        if(token != null && !token.equals("")){
            try{
                String validToken = jwtUtils.refresh(token);
                httpServletResponse.setHeader("x-bearer-token", validToken);
            }catch (JWTVerificationException ignore){

            }
        }

        filterChain.doFilter(httpServletRequest, httpServletResponse);
    }
}
