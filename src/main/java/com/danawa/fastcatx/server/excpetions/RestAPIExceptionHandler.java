package com.danawa.fastcatx.server.excpetions;

import com.danawa.fastcatx.server.entity.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.io.IOException;

@ControllerAdvice
public class RestAPIExceptionHandler extends ResponseEntityExceptionHandler {
    private static Logger logger = LoggerFactory.getLogger(RestAPIExceptionHandler.class);


    @ExceptionHandler(value = {PermissionException.class})
    protected ResponseEntity<Object> handlePermissionException(Exception ex, WebRequest request) {
        logger.error("[SYSTEM ERROR]", ex);
        return handleExceptionInternal(ex,
                new ErrorResponse(ex.getMessage()),
                new HttpHeaders(),
                HttpStatus.UNAUTHORIZED,
                request);
    }

    @ExceptionHandler(value = {NotFoundException.class})
    protected ResponseEntity<Object> handleNotFoundUserException(Exception ex, WebRequest request) {
        logger.error("[AUTH ERROR]", ex);
        return handleExceptionInternal(ex,
                new ErrorResponse(ex.getMessage()),
                new HttpHeaders(),
                HttpStatus.BAD_REQUEST,
                request);
    }

    @ExceptionHandler(value = {
            Exception.class,
            ServiceException.class,
            IOException.class
    })
    protected ResponseEntity<Object> handleServiceException(Exception ex, WebRequest request) {
        logger.error("[SYSTEM ERROR]", ex);
        return handleExceptionInternal(ex,
                new ErrorResponse(ex.getCause().getMessage()),
                new HttpHeaders(),
                HttpStatus.INTERNAL_SERVER_ERROR,
                request);
    }

}