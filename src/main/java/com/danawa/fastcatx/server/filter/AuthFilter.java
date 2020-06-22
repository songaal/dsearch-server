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

@Component
public class AuthFilter extends OncePerRequestFilter {
    private static Logger logger = LoggerFactory.getLogger(AuthFilter.class);


    @Override
    protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, FilterChain filterChain) throws ServletException, IOException {
        HttpSession session = httpServletRequest.getSession();
        String sessionId = session.getId();
        AuthUser authUser = (AuthUser) session.getAttribute(AuthController.SESSION_KEY);
        logger.debug("sessionId: {}, URI: {}", sessionId, httpServletRequest.getRequestURI());
        filterChain.doFilter(httpServletRequest, httpServletResponse);
    }


}
