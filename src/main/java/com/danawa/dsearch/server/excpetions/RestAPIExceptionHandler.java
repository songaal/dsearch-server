package com.danawa.dsearch.server.excpetions;

import com.danawa.dsearch.server.entity.ErrorResponse;
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

    @ExceptionHandler(value = {
            NotFoundException.class,
            DuplicateException.class
    })
    protected ResponseEntity<Object> handleNotFoundException(Exception ex, WebRequest request) {
        return handleExceptionInternal(ex,
                new ErrorResponse(ex.getMessage()),
                new HttpHeaders(),
                HttpStatus.BAD_REQUEST,
                request);
    }

    @ExceptionHandler(value = {NotFoundUserException.class})
    protected ResponseEntity<Object> handleNotFoundUserException(Exception ex, WebRequest request) {
        logger.debug("[AUTH ERROR] {}", ex.getMessage());
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