package com.practicekafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import com.practicekafka.entity.FailureRecord;
import com.practicekafka.jpa.FailureRecordRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class FailureService {

    private FailureRecordRepository failureRecordRepository;

    // auto wired or constructor injection are both availbel
    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String status) {

        var failureRecord = new FailureRecord(null, consumerRecord.topic(),
                consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset(),
                e.getCause().getMessage(), status);
        failureRecordRepository.save(failureRecord);
    }
}
