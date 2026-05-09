package com.github.dimitryivaniuta.gateway.leaderboard.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

/**
 * Reactive transaction infrastructure used by PostgreSQL adapters.
 */
@Configuration
public class R2dbcTransactionConfig {

    /**
     * Creates a transactional operator for composing R2DBC operations in one transaction.
     *
     * @param transactionManager reactive transaction manager auto-configured by Spring Boot
     * @return transactional operator
     */
    @Bean
    public TransactionalOperator transactionalOperator(ReactiveTransactionManager transactionManager) {
        return TransactionalOperator.create(transactionManager);
    }
}
