package com.danawa.dsearch.server.filter;

import com.danawa.dsearch.server.controller.AuthController;
import com.danawa.dsearch.server.entity.AuthUser;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@Component
public class AuthFilter implements Filter {
    private static Logger logger = LoggerFactory.getLogger(AuthFilter.class);

    // 미인증 URI
    private static final List<String> bypassUri = Arrays.asList(
            "/", "/info",
            "/collections/idxp", "/collections/idxp/status", // IndexProcess용
            "/auth/sign-in", "/auth/sign-out"
    );
    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) req;
        HttpServletResponse response = (HttpServletResponse) resp;

        String uri = request.getRequestURI();
        HttpSession session = request.getSession();
        AuthUser authUser = (AuthUser) session.getAttribute(AuthController.SESSION_KEY);
        logger.trace("session: {}, uri: {}", session.getId(), uri);
        if (authUser != null) {
            chain.doFilter(req, resp);
            addSameSite(response);
        } else if (bypassUri.contains(uri)) {
            chain.doFilter(req, resp);
            addSameSite(response);
        } else {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    /* for SameSite=None */
    private void addSameSite(HttpServletResponse response){
        boolean firstHeader= true;
        Collection<String> headers = response.getHeaders(HttpHeaders.SET_COOKIE);
        for(String header: headers){
            if(firstHeader){
                response.setHeader(HttpHeaders.SET_COOKIE, String.format("%s; Secure; %s", header, "SameSite=None"));
                firstHeader = false;
                continue;
            }else{
                response.addHeader(HttpHeaders.SET_COOKIE, String.format("%s; Secure; %s", header, "SameSite=None"));
            }
        }
    }

}
