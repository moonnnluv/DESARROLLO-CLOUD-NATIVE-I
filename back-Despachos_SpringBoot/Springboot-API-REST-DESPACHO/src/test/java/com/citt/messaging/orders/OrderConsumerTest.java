package com.citt.messaging.orders;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.rabbitmq.client.Channel;

/**
 * Verifica la decision que toma el consumidor en cada ruta. No necesita RabbitMQ:
 * el canal y el procesador son mocks (Mockito).
 */
@ExtendWith(MockitoExtension.class)
class OrderConsumerTest {

    private static final long TAG = 7L;
    private static final String ORDER = "Orden ID: ORD-1 | Cliente: Ana | Hora: 10:00:00";

    @Mock
    private OrderProcessor orderProcessor;

    @Mock
    private Channel channel;

    @InjectMocks
    private OrderConsumer consumer;

    @Test
    void ordenProcesadaHaceAck() throws IOException {
        consumer.processOrder(ORDER, channel, TAG, false);

        verify(channel).basicAck(TAG, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void falloTemporalEnPrimeraEntregaReencolaParaReintentar() throws IOException {
        doThrow(new RecoverableOrderException("temporal")).when(orderProcessor).process(anyString());

        consumer.processOrder(ORDER, channel, TAG, false);

        verify(channel).basicNack(TAG, false, true);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void falloTemporalYaReintentadoVaALaDlq() throws IOException {
        doThrow(new RecoverableOrderException("temporal")).when(orderProcessor).process(anyString());

        consumer.processOrder(ORDER, channel, TAG, true);

        verify(channel).basicNack(TAG, false, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void ordenInvalidaVaDirectoALaDlq() throws IOException {
        doThrow(new UnrecoverableOrderException("invalida")).when(orderProcessor).process(anyString());

        consumer.processOrder(ORDER, channel, TAG, false);

        verify(channel).basicNack(TAG, false, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void errorInesperadoVaALaDlqSinReencolar() throws IOException {
        doThrow(new IllegalArgumentException("inesperado")).when(orderProcessor).process(anyString());

        consumer.processOrder(ORDER, channel, TAG, false);

        verify(channel).basicNack(TAG, false, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void consumidorDeDlqRegistraYHaceAck() throws IOException {
        consumer.processDLQ(ORDER, channel, TAG);

        verify(channel).basicAck(TAG, false);
        verifyNoMoreInteractions(channel);
    }
}
