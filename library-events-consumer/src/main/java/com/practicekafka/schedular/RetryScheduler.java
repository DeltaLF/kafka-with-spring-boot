package com.practicekafka.schedular;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.practicekafka.config.LibraryEventsConsumerConfig;
import com.practicekafka.entity.FailureRecord;
import com.practicekafka.jpa.FailureRecordRepository;
import com.practicekafka.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class RetryScheduler {

    private FailureRecordRepository failureRecordRepository;
    private LibraryEventsService libraryEventsService;

    public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventsService libraryEventsService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventsService = libraryEventsService;
    }

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords() {
        log.info("### start to retry failed records");
        var failureRecords = failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY);

        failureRecords.forEach(failureRecord -> {

            var consumerRecord = buildConsumerRecord(failureRecord);

            try {
                log.info("### failed records to be retied: {}", failureRecord);

                libraryEventsService.processLibraryEvent(consumerRecord);
                failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                failureRecordRepository.save(failureRecord); // update staus to success
            } catch (Exception e) {
                log.error("Exception in retryFailedrecords: {}", failureRecord);
            }
        });
        log.info("### retry failed records completed");

    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKey_value(),
                failureRecord.getErrorRecord());
    }

}
