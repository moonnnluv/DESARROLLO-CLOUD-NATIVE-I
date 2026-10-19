package com.citt.rabbitadmin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.citt.rabbitadmin.dto.BindingRequest;
import com.citt.rabbitadmin.dto.ExchangeRequest;
import com.citt.rabbitadmin.dto.ExchangeType;
import com.citt.rabbitadmin.dto.QueueRequest;
import com.citt.rabbitadmin.exceptions.InvalidRabbitResourceException;
import com.citt.rabbitadmin.exceptions.ResourceAlreadyExistsException;
import com.citt.rabbitadmin.exceptions.ResourceNotFoundException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;

class RabbitAdminServiceTest {

    private AmqpAdmin amqpAdmin;
    private RabbitAdminService service;

    @BeforeEach
    void setUp() {
        amqpAdmin = mock(AmqpAdmin.class);
        service = new RabbitAdminService(amqpAdmin);
    }

    @Test
    void createQueue_declaraColaDurableConArgumentos() {
        when(amqpAdmin.getQueueProperties("orders.queue")).thenReturn(null);

        service.createQueue(new QueueRequest("orders.queue", null, 30000L, 1000, "orders.dlx", "order.dead"));

        ArgumentCaptor<Queue> captor = ArgumentCaptor.forClass(Queue.class);
        verify(amqpAdmin).declareQueue(captor.capture());
        Queue queue = captor.getValue();
        assertEquals("orders.queue", queue.getName());
        assertEquals(true, queue.isDurable());
        assertEquals(30000L, queue.getArguments().get("x-message-ttl"));
        assertEquals("orders.dlx", queue.getArguments().get("x-dead-letter-exchange"));
        assertEquals("order.dead", queue.getArguments().get("x-dead-letter-routing-key"));
    }

    @Test
    void createQueue_siYaExiste_lanzaConflicto() {
        when(amqpAdmin.getQueueProperties("q1")).thenReturn(new Properties());

        assertThrows(ResourceAlreadyExistsException.class,
                () -> service.createQueue(new QueueRequest("q1", null, null, null, null, null)));
        verify(amqpAdmin, never()).declareQueue(any());
    }

    @Test
    void createQueue_nombreReservado_lanzaInvalido() {
        assertThrows(InvalidRabbitResourceException.class,
                () -> service.createQueue(new QueueRequest("amq.interna", null, null, null, null, null)));
    }

    @Test
    void createQueue_routingKeySinExchange_lanzaInvalido() {
        assertThrows(InvalidRabbitResourceException.class,
                () -> service.createQueue(new QueueRequest("q1", null, null, null, null, "order.dead")));
    }

    @Test
    void deleteQueue_siNoExiste_lanzaNotFound() {
        when(amqpAdmin.getQueueProperties("nada")).thenReturn(null);

        assertThrows(ResourceNotFoundException.class, () -> service.deleteQueue("nada"));
        verify(amqpAdmin, never()).deleteQueue(anyString());
    }

    @Test
    void deleteQueue_siExiste_laElimina() {
        when(amqpAdmin.getQueueProperties("q1")).thenReturn(new Properties());

        service.deleteQueue("q1");

        verify(amqpAdmin).deleteQueue("q1");
    }

    @Test
    void createExchange_declaraElTipoPedido() {
        service.createExchange(new ExchangeRequest("orders.dlx", ExchangeType.FANOUT, null));

        ArgumentCaptor<Exchange> captor = ArgumentCaptor.forClass(Exchange.class);
        verify(amqpAdmin).declareExchange(captor.capture());
        assertEquals("fanout", captor.getValue().getType());
        assertEquals(true, captor.getValue().isDurable());
    }

    @Test
    void createBinding_siLaColaNoExiste_lanzaNotFound() {
        when(amqpAdmin.getQueueProperties("q1")).thenReturn(null);

        assertThrows(ResourceNotFoundException.class,
                () -> service.createBinding(new BindingRequest("q1", "ex1", "rk")));
    }

    @Test
    void createBinding_declaraElBinding() {
        when(amqpAdmin.getQueueProperties("q1")).thenReturn(new Properties());

        service.createBinding(new BindingRequest("q1", "ex1", "rk"));

        ArgumentCaptor<Binding> captor = ArgumentCaptor.forClass(Binding.class);
        verify(amqpAdmin).declareBinding(captor.capture());
        assertEquals("q1", captor.getValue().getDestination());
        assertEquals("ex1", captor.getValue().getExchange());
        assertEquals("rk", captor.getValue().getRoutingKey());
    }
}
