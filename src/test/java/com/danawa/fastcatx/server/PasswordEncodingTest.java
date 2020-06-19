package com.danawa.fastcatx.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;

public class PasswordEncodingTest {
    private static Logger logger = LoggerFactory.getLogger(PasswordEncodingTest.class);

    public static void main(String[] args) {

        PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        String password = "나는 준우다.";

        String basicEnc = encoder.encode(password);
        String sha256Enc = encoder.encode("{sha256}" + password);
        String md5Enc = encoder.encode("{MD5}" + password);

        logger.debug("basicEnc: {}", basicEnc);
        logger.debug("sha256Enc: {}", sha256Enc);
        logger.debug("md5Enc: {}", md5Enc);

        logger.debug("basicEnc isTrue: {}", encoder.matches(password, basicEnc));
        logger.debug("sha256Enc isTrue: {}", encoder.matches(password, sha256Enc));
        logger.debug("md5Enc isTrue: {}", encoder.matches(password, md5Enc));

        logger.debug("encodingId basicEnc isTrue: {}", encoder.matches(password, basicEnc));
        logger.debug("encodingId sha256Enc isTrue: {}", encoder.matches("{sha256}" + password, sha256Enc));
        logger.debug("encodingId md5Enc isTrue: {}", encoder.matches("{MD5}" + password, md5Enc));

    }

}
