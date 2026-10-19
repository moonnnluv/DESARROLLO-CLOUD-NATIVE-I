package com.citt.rabbitadmin.exceptions;

import com.citt.rabbitadmin.dto.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;

import java.util.stream.Collectors;

/**
 * Traduce excepciones a respuestas HTTP uniformes (patron Controller Advice).
 * Asi los controllers no tienen try/catch y todos los errores tienen el mismo formato.
 */
@RestControllerAdvice
public class RestExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RestExceptionHandler.class);

    @ExceptionHandler(BindException.class)
    public ResponseEntity<ApiMessage> handleBody(BindException ex) {
        String detail = ex.getBindingResult().getFieldErrors().stream()
                .map((FieldError e) -> e.getField() + ": " + e.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining("; "));
        return build(HttpStatus.BAD_REQUEST, detail);
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<ApiMessage> handleParams(HandlerMethodValidationException ex) {
        String detail = ex.getAllErrors().stream()
                .map(MessageSourceResolvable::getDefaultMessage)
                .collect(Collectors.joining("; "));
        return build(HttpStatus.BAD_REQUEST, detail);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiMessage> handleUnreadable(HttpMessageNotReadableException ex) {
        return build(HttpStatus.BAD_REQUEST, "El cuerpo de la peticion no es un JSON valido o tiene un valor no permitido");
    }

    @ExceptionHandler(InvalidRabbitResourceException.class)
    public ResponseEntity<ApiMessage> handleInvalid(InvalidRabbitResourceException ex) {
        return build(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ApiMessage> handleNotFound(ResourceNotFoundException ex) {
        return build(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(ResourceAlreadyExistsException.class)
    public ResponseEntity<ApiMessage> handleConflict(ResourceAlreadyExistsException ex) {
        return build(HttpStatus.CONFLICT, ex.getMessage());
    }

    /** Fallos al hablar con el broker (caido, argumentos incompatibles, exchange inexistente, etc.). */
    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<ApiMessage> handleBroker(AmqpException ex) {
        log.error("Error comunicandose con RabbitMQ", ex);
        return build(HttpStatus.BAD_GATEWAY, "RabbitMQ rechazo la operacion o no esta disponible");
    }

    private ResponseEntity<ApiMessage> build(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(ApiMessage.of(status.value(), message));
    }
}
