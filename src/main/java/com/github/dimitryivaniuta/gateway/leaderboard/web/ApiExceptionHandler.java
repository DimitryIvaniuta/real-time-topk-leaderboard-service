package com.github.dimitryivaniuta.gateway.leaderboard.web;

import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

/**
 * Converts application exceptions into RFC 9457 problem details.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    /**
     * Handles invalid user input and returns a 400 problem response.
     *
     * @param exception validation exception
     * @return problem detail
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, exception.getMessage());
        problem.setType(URI.create("https://errors.leaderboard.local/invalid-request"));
        problem.setTitle("Invalid request");
        return problem;
    }

    /**
     * Handles explicit response status exceptions.
     *
     * @param exception response status exception
     * @return problem detail
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ProblemDetail handleResponseStatus(ResponseStatusException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(exception.getStatusCode(), exception.getReason());
        problem.setType(URI.create("https://errors.leaderboard.local/request-failed"));
        problem.setTitle("Request failed");
        return problem;
    }
}
