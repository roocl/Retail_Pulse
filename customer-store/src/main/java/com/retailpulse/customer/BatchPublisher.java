package com.retailpulse.customer;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

final class BatchPublisher {
    enum Kind {
        PROFILE("profile_current","profile_batches","dataset"), SCORE("score_current","score_batches","profile_batch_id");
        final String head,batches,scope;
        Kind(String head,String batches,String scope){this.head=head;this.batches=batches;this.scope=scope;}
    }
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    BatchPublisher(JdbcTemplate jdbc,TransactionTemplate transaction){this.jdbc=jdbc;this.transaction=transaction;}
    void publish(Kind kind,String scope,String id,String hash,Runnable insert,Runnable verify) {
        transaction.executeWithoutResult(status->{
            jdbc.update("INSERT INTO "+kind.head+"("+kind.scope+") VALUES (?) ON DUPLICATE KEY UPDATE "+kind.scope+"=VALUES("+kind.scope+")",scope);
            jdbc.queryForList("SELECT batch_id FROM "+kind.head+" WHERE "+kind.scope+"=? FOR UPDATE",scope);
            var existing=jdbc.queryForList("SELECT content_hash FROM "+kind.batches+" WHERE id=?",String.class,id);
            if(!existing.isEmpty()) {
                if(!existing.get(0).equals(hash))throw new IllegalStateException("Batch identity already has different content");
                verify.run();
                return;
            }
            insert.run();
            verify.run();
            jdbc.update("UPDATE "+kind.head+" SET batch_id=? WHERE "+kind.scope+"=?",id,scope);
        });
    }
}
