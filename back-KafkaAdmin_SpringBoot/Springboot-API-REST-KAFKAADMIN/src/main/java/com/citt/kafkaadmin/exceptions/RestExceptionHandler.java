package com.citt.kafkaadmin.exceptions;

import com.citt.kafkaadmin.dto.ApiMessage;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * Kafka informa sus errores con excepciones propias; aqui se convierten en el codigo HTTP que corresponde.
 */
@RestControllerAdvice
public class RestExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RestExceptionHandler.class);

    // ------------------------------------------------ validacion de la peticion (400)

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

    @ExceptionHandler(InvalidKafkaResourceException.class)
    public ResponseEntity<ApiMessage> handleInvalid(InvalidKafkaResourceException ex) {
        return build(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    // ------------------------------------------------ errores que informa Kafka

    /** Kafka explica bien estos casos (ej. "Topic already has 3 partitions"), por eso se reenvia su mensaje. */
    @ExceptionHandler({
            InvalidTopicException.class,
            InvalidReplicationFactorException.class,
            InvalidPartitionsException.class,
            InvalidConfigurationException.class,
            InvalidRequestException.class})
    public ResponseEntity<ApiMessage> handleKafkaInvalid(KafkaException ex) {
        return build(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(UnknownTopicOrPartitionException.class)
    public ResponseEntity<ApiMessage> handleUnknownTopic(UnknownTopicOrPartitionException ex) {
        return build(HttpStatus.NOT_FOUND, "El topico no existe");
    }

    @ExceptionHandler(GroupIdNotFoundException.class)
    public ResponseEntity<ApiMessage> handleUnknownGroup(GroupIdNotFoundException ex) {
        return build(HttpStatus.NOT_FOUND, "El consumer group no existe");
    }

    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<ApiMessage> handleNotFound(ResourceNotFoundException ex) {
        return build(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(TopicExistsException.class)
    public ResponseEntity<ApiMessage> handleTopicExists(TopicExistsException ex) {
        return build(HttpStatus.CONFLICT, "El topico ya existe");
    }

    // ------------------------------------------------ cluster caido o lento (502)

    @ExceptionHandler(KafkaUnavailableException.class)
    public ResponseEntity<ApiMessage> handleUnavailable(KafkaUnavailableException ex) {
        log.error("Kafka no disponible", ex);
        return build(HttpStatus.BAD_GATEWAY, ex.getMessage());
    }

    /** Cualquier otro fallo al hablar con los brokers (timeouts, autorizacion, etc.). */
    @ExceptionHandler(KafkaException.class)
    public ResponseEntity<ApiMessage> handleKafka(KafkaException ex) {
        log.error("Error comunicandose con Kafka", ex);
        return build(HttpStatus.BAD_GATEWAY, "Kafka rechazo la operacion o no esta disponible");
    }

    private ResponseEntity<ApiMessage> build(HttpStatus status, String message) {
        return ResponseEntity.status(status).body(ApiMessage.of(status.value(), message));
    }
}
