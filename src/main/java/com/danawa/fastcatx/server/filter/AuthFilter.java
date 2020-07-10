package com.danawa.fastcatx.server.filter;

import com.danawa.fastcatx.server.controller.AuthController;
import com.danawa.fastcatx.server.entity.AuthUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class AuthFilter extends OncePerRequestFilter {
    private static Logger logger = LoggerFactory.getLogger(AuthFilter.class);

    private static final List<String> bypassUri = Arrays.asList(
            "/",
            "/auth", "/auth/sign-in", "/auth/sign-out"
    );

    @Override
    protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, FilterChain filterChain) throws ServletException, IOException {
        HttpSession session = httpServletRequest.getSession();
        String sessionId = session.getId();
        AuthUser authUser = (AuthUser) session.getAttribute(AuthController.SESSION_KEY);
        logger.debug("sessionId: {}, isAuth: {}, URI: {}", sessionId, authUser != null, httpServletRequest.getRequestURI());

        String uri = httpServletRequest.getRequestURI();
        if (!bypassUri.contains(uri) && authUser == null) {
            httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        }
        filterChain.doFilter(httpServletRequest, httpServletResponse);
    }


}
