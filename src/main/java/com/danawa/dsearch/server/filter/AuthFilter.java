package com.danawa.dsearch.server.filter;

import com.danawa.dsearch.server.utils.JWTUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Component
public class AuthFilter implements Filter {
    private static Logger logger = LoggerFactory.getLogger(AuthFilter.class);

    private JWTUtils jwtUtils;

    // 미인증 URI
    public static final List<String> bypassUri = Arrays.asList(
            "/", "/info",
            "/collections/idxp", "/collections/idxp/status", "/collections/setTimeout",
            "/collections/getSettings", "/collections/setSettings", // IndexProcess용
            "/auth/sign-in", "/auth/sign-out", "/dictionaries/remote"
    );

    @Autowired
    public AuthFilter(Environment env) {
        String secret = env.getRequiredProperty("dsearch.auth.secret");
        long expirationTimeMillis = Long.parseLong(env.getRequiredProperty("dsearch.auth.expiration-time-millis"));
        this.jwtUtils = new JWTUtils(secret, expirationTimeMillis);
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) req;
        HttpServletResponse response = (HttpServletResponse) resp;

        String uri = request.getRequestURI();
        if (bypassUri.contains(uri)) {
            chain.doFilter(req, resp);
        } else {
            String token = request.getHeader("x-bearer-token");
            if (jwtUtils.verify(token)) {
                chain.doFilter(req, resp);
            } else {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            }

        }

//        String uri = request.getRequestURI();
//        HttpSession session = request.getSession();
//        AuthUser authUser = (AuthUser) session.getAttribute(AuthController.SESSION_KEY);
//        logger.trace("session: {}, uri: {}", session.getId(), uri);
//        if (authUser != null) {
//            chain.doFilter(req, resp);
//            addSameSite(response);
//        } else if (bypassUri.contains(uri)) {
//            chain.doFilter(req, resp);
//            addSameSite(response);
//        } else {
//            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
//        }
    }



}
