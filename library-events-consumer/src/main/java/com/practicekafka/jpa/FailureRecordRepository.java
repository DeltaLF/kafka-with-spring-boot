package com.practicekafka.jpa;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

import com.practicekafka.entity.FailureRecord;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {

    List<FailureRecord> findAllByStatus(String retry);
}
