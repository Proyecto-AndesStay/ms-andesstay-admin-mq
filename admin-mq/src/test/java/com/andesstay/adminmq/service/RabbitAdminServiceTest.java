package com.andesstay.adminmq.service;

import com.andesstay.adminmq.dto.BindingRequestDto;
import com.andesstay.adminmq.dto.ExchangeRequestDto;
import com.andesstay.adminmq.dto.QueueRequestDto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RabbitAdminServiceTest {

    @Mock
    private RabbitAdmin rabbitAdmin;

    @InjectMocks
    private RabbitAdminService service;

    @Test
    void createQueue_ConDlq_DeberiaDeclararDlqYColaPrincipal() {
        QueueRequestDto dto = mock(QueueRequestDto.class);
        when(dto.getName()).thenReturn("q.cmd.email");
        when(dto.getDlqName()).thenReturn("q.cmd.email.dlq");
        when(dto.isDurable()).thenReturn(true);
        when(dto.isAutoDelete()).thenReturn(false);

        service.createQueue(dto);

        verify(rabbitAdmin, times(2)).declareQueue(any(Queue.class));
    }

    @Test
    void createExchange_DeberiaDeclararDirectYTopic() {
        ExchangeRequestDto directDto = mock(ExchangeRequestDto.class);
        when(directDto.getName()).thenReturn("cmd.direct");
        when(directDto.getType()).thenReturn("direct");
        when(directDto.isDurable()).thenReturn(true);

        ExchangeRequestDto topicDto = mock(ExchangeRequestDto.class);
        when(topicDto.getName()).thenReturn("cmd.topic");
        when(topicDto.getType()).thenReturn("topic");
        when(topicDto.isDurable()).thenReturn(true);

        service.createExchange(directDto);
        service.createExchange(topicDto);

        verify(rabbitAdmin, times(1)).declareExchange(any(DirectExchange.class));
        verify(rabbitAdmin, times(1)).declareExchange(any(TopicExchange.class));
    }

    @Test
    void createBinding_DeberiaDeclararBindingCorrectamente() {
        BindingRequestDto dto = mock(BindingRequestDto.class);
        when(dto.getQueueName()).thenReturn("q.cmd.email");
        when(dto.getExchangeName()).thenReturn("cmd.direct");
        when(dto.getRoutingKey()).thenReturn("email.send");

        service.createBinding(dto);

        verify(rabbitAdmin, times(1)).declareBinding(any(Binding.class));
    }

    @Test
    void deleteQueueYExchange_DeberianRetornarTrue() {
        when(rabbitAdmin.deleteQueue("q.cmd.email")).thenReturn(true);
        when(rabbitAdmin.deleteExchange("cmd.direct")).thenReturn(true);

        assertTrue(service.deleteQueue(" q.cmd.email "));
        assertTrue(service.deleteExchange(" cmd.direct "));
    }
}