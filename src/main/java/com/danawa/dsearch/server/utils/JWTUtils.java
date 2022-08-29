package com.danawa.dsearch.server.utils;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTCreationException;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.danawa.dsearch.server.auth.entity.AuthUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class JWTUtils {
    private static Logger logger = LoggerFactory.getLogger(JWTUtils.class);

    private static final String ISSUER = "DSearch-Server";
    private final String secret;
    private final Long expirationTimeMillis;
    private Map<String, Object> headerClaims = new HashMap<>();

    public static final String USER_ID = "user_id";

    public JWTUtils(String secret, Long expirationTimeMillis) {
        this.secret = secret;
        this.expirationTimeMillis = expirationTimeMillis;
        this.headerClaims.put("typ", "JWT");
        this.headerClaims.put("alg", "HS256");
    }
    public String sign(AuthUser authUser) {
        String token = null;
        Algorithm algorithm = Algorithm.HMAC256(secret);
        try {
            token = JWT.create()
                    .withIssuer(ISSUER)
                    .withClaim(USER_ID, authUser.getUser().getId())
                    .withHeader(headerClaims)
                    .withExpiresAt(new Date(System.currentTimeMillis() + expirationTimeMillis))
                    .sign(algorithm);
        } catch (JWTCreationException exception){
            logger.error("generate jwt token fail. {}", authUser);
            logger.error("exception >> ", exception);
        }
        return token;
    }

    public boolean verify(String token) {
        if (token == null) {
            return false;
        }
        try {
            Algorithm algorithm = Algorithm.HMAC256(secret);
            JWTVerifier verifier = JWT.require(algorithm).build();
            verifier.verify(token);
            return true;
        } catch (JWTVerificationException exception) {
            logger.trace("expired token: {}", exception.getMessage());
            return false;
        }
    }

    public String refresh(String token) {
        String validToken = "";
        Algorithm algorithm = Algorithm.HMAC256(secret);
        DecodedJWT jwt = JWT.decode(token);
        // 갱신
        long refreshDate = jwt.getExpiresAt().getTime() - (expirationTimeMillis / 2);
        if (System.currentTimeMillis() >= refreshDate) {
            validToken = JWT.create()
                    .withIssuer(ISSUER)
                    .withClaim(USER_ID, jwt.getClaim(USER_ID).asLong())
                    .withHeader(headerClaims)
                    .withExpiresAt(new Date(System.currentTimeMillis() + expirationTimeMillis))
                    .sign(algorithm);
        } else {
            validToken = token;
        }
        return validToken;
    }

    public Claim getClaims(String token, String key) {
        if(!verify(token)) {
            return null;
        }
        DecodedJWT jwt = JWT.decode(token);
        return jwt.getClaim(key);
    }

    public String getSecret() {
        return secret;
    }

    public Long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }
}
