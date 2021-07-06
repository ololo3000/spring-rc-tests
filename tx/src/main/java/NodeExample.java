/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.spring.CacheSpringStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.spring.IgniteClientSpringTransactionManager;
import org.apache.ignite.transactions.spring.SpringTransactionManager;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/** Represents example of using Ignite Spring Transactions integration with the thin client. */
public class NodeExample {
    /** Ignite cache name. */
    public static final String ACCOUNT_CACHE_NAME = "example-account-cache";

    /** */
    public static void main(String[] args) {
        CacheSpringStoreSessionListener listener = new CacheSpringStoreSessionListener();
        try (
            Ignite ignored = Ignition.start(); // Starts an Ignite cluster consisting of one server node.
            AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()
        ) {
            ctx.register(SpringApplicationConfiguration.class);
            ctx.refresh();

            IgniteTransactionalService svc = ctx.getBean(IgniteTransactionalService.class);

            svc.createAccount("Bob", 1000);
            svc.createAccount("Alice", 100);
            svc.createAccount("Eve", 0);
            svc.createAccount("Dave", 0);

            doFundTransferWithBroker(svc, "Bob", "Alice", "Eve", "Dave", 1000, 10);

            doFundTransferWithBroker(svc, "Bob", "Alice", "Eve", "Dave", 100, 10);
        }
    }

    /** Delegates funds transfer operation to {@link IgniteClientTransactionalService} and logs the result. */
    private static void doFundTransferWithBroker(
        IgniteTransactionalService svc,
        String firstEmitter,
        String secondEmitter,
        String recipient,
        String broker,
        int cash,
        int fee
    ) {
        System.out.println("+--------------Fund transfer operation--------------+");

        try {
            svc.transferFundsWithBroker(firstEmitter, secondEmitter, recipient, broker, cash, fee);

            System.out.println(">>> Operation completed successfully");
        }
        catch (RuntimeException e) {
            System.out.println(">>> Operation was rolled back [error = " + e.getMessage() + ']');
        }

        System.out.println("\n>>> Account statuses:");

        System.out.println(">>> " + firstEmitter + " balance: " + svc.getBalance(firstEmitter));
        System.out.println(">>> " + secondEmitter + " balance: " + svc.getBalance(secondEmitter));
        System.out.println(">>> " + recipient + " balance: " + svc.getBalance(recipient));
        System.out.println(">>> " + broker + " balance: " + svc.getBalance(broker));
        System.out.println("+---------------------------------------------------+");
    }

    /** Spring application configuration. */
    @Configuration
    @EnableTransactionManagement
    public static class SpringApplicationConfiguration {
        /**
         * Ignite thin client instance that will be used to both initialize
         * {@link IgniteClientSpringTransactionManager} and perform transactional cache operations.
         */
        @Bean
        public Ignite ignite() {
            return Ignition.start(new IgniteConfiguration().setIgniteInstanceName("ignite-node"));
        }

        /** Ignite implementation of the Spring Transactions manager interface. */
        @Bean
        public SpringTransactionManager transactionManager(Ignite cli) {
            SpringTransactionManager mgr = new SpringTransactionManager();

            mgr.setTransactionConcurrency(PESSIMISTIC);
            mgr.setIgniteInstanceName("ignite-node");

            return mgr;
        }

        /** Service instance that uses declarative transaction management when working with the Ignite cache. */
        @Bean
        public IgniteTransactionalService transactionalService(Ignite cli) {
            IgniteTransactionalService svc = new IgniteTransactionalService();

            svc.setCache(cli.getOrCreateCache(new CacheConfiguration()
                .setName(ACCOUNT_CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL)));

            return svc;
        }
    }
}
