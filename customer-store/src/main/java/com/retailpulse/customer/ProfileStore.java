package com.retailpulse.customer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

public final class ProfileStore {
    private final DataSource source;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private static final RowMapper<Profile> PROFILE = (r,n) -> new Profile(r.getString("customer_id"),r.getString("country"),
            r.getInt("recency_days"),r.getLong("orders"),r.getBigDecimal("purchase_amount"),r.getBigDecimal("cancellation_amount"),
            r.getString("preferred_product"),r.getInt("r_score"),r.getInt("f_score"),r.getInt("m_score"),r.getString("segment"));
    private static final RowMapper<ProfileBatch> BATCH = (r,n) -> new ProfileBatch(r.getString("id"),r.getString("dataset"),
            r.getString("source_release"),r.getString("rule_version"),r.getDate("observation").toLocalDate(),r.getInt("window_days"));

    public ProfileStore(DataSource source) {
        this.source=source;
        jdbc=new JdbcTemplate(source);
        jdbc.setQueryTimeout(15);
        transaction=new TransactionTemplate(new DataSourceTransactionManager(source));
    }

    public void initialize() {
        new ResourceDatabasePopulator(new ClassPathResource("customer/schema.sql")).execute(source);
    }

    public void publish(ProfileBatch batch, List<Profile> profiles) {
        var sorted=profiles.stream().sorted(Comparator.comparing(Profile::customerId)).toList();
        final String hash;
        try {
            var digest=MessageDigest.getInstance("SHA-256");
            digest.update(batch.toString().getBytes(StandardCharsets.UTF_8));
            hash=HexFormat.of().formatHex(digest.digest(new ObjectMapper().writeValueAsBytes(sorted)));
        } catch (Exception error) { throw new IllegalStateException("Cannot identify profile content",error); }
        transaction.executeWithoutResult(status -> {
            jdbc.update("INSERT INTO profile_current(dataset) VALUES (?) ON DUPLICATE KEY UPDATE dataset=VALUES(dataset)",batch.dataset());
            jdbc.queryForList("SELECT batch_id FROM profile_current WHERE dataset=? FOR UPDATE",batch.dataset());
            var existing=jdbc.queryForList("SELECT content_hash FROM profile_batches WHERE id=?",String.class,batch.id());
            if (!existing.isEmpty()) {
                if (!existing.get(0).equals(hash)) throw new IllegalStateException("Batch identity already has different content");
                if (!jdbc.query("SELECT * FROM customer_profiles WHERE batch_id=? ORDER BY customer_id",PROFILE,batch.id()).equals(sorted))
                    throw new IllegalStateException("Published customer data is inconsistent");
                return;
            }
            jdbc.update("INSERT INTO profile_batches(id,dataset,source_release,rule_version,observation,window_days,content_hash,customer_count) VALUES (?,?,?,?,?,?,?,?)",
                    batch.id(),batch.dataset(),batch.sourceRelease(),batch.ruleVersion(),batch.observation(),batch.windowDays(),hash,sorted.size());
            jdbc.batchUpdate("INSERT INTO customer_profiles VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",sorted,500,(statement,p) -> {
                statement.setString(1,batch.id());statement.setString(2,p.customerId());statement.setString(3,p.country());
                statement.setInt(4,p.recencyDays());statement.setLong(5,p.orders());statement.setBigDecimal(6,p.purchaseAmount());
                statement.setBigDecimal(7,p.cancellationAmount());statement.setString(8,p.preferredProduct());
                statement.setInt(9,p.rScore());statement.setInt(10,p.fScore());statement.setInt(11,p.mScore());statement.setString(12,p.segment());
            });
            jdbc.update("UPDATE profile_current SET batch_id=? WHERE dataset=?",batch.id(),batch.dataset());
        });
    }

    public record Page(ProfileBatch batch,List<Profile> items,boolean hasMore,String nextAfter) {}

    public Page list(String dataset,String batchId,String after,String segment,String customerId,int limit) {
        if(limit<1 || limit>100) throw new IllegalArgumentException("Limit must be between 1 and 100");
        var batches=batchId==null
                ? jdbc.query("SELECT b.* FROM profile_batches b JOIN profile_current c ON c.batch_id=b.id WHERE c.dataset=?",BATCH,dataset)
                : jdbc.query("SELECT * FROM profile_batches WHERE dataset=? AND id=?",BATCH,dataset,batchId);
        if(batches.isEmpty()) {
            if(batchId!=null) throw new IllegalArgumentException("Unknown dataset or batch");
            return new Page(null,List.of(),false,null);
        }
        var batch=batches.get(0);
        StringBuilder sql=new StringBuilder("SELECT * FROM customer_profiles WHERE batch_id=?");
        var args=new ArrayList<Object>();args.add(batch.id());
        if(after!=null){sql.append(" AND customer_id>?");args.add(after);}
        if(segment!=null){sql.append(" AND segment=?");args.add(segment);}
        if(customerId!=null){sql.append(" AND customer_id=?");args.add(customerId);}
        sql.append(" ORDER BY customer_id LIMIT ?");args.add(limit+1);
        var rows=jdbc.query(sql.toString(),PROFILE,args.toArray());
        boolean more=rows.size()>limit;
        var items=List.copyOf(rows.subList(0,Math.min(limit,rows.size())));
        return new Page(batch,items,more,more?items.get(items.size()-1).customerId():null);
    }

    public Optional<Profile> find(String dataset,String batchId,String customerId) {
        return list(dataset,batchId,null,null,customerId,1).items().stream().findFirst();
    }

    public List<java.util.Map<String,Object>> summary(String dataset,String batchId) {
        var batch=list(dataset,batchId,null,null,null,1).batch();
        if(batch==null) return List.of();
        return jdbc.queryForList("""
                SELECT segment,COUNT(*) AS customers,SUM(purchase_amount) AS purchase_amount,
                       SUM(cancellation_amount) AS cancellation_amount,SUM(orders) AS orders
                FROM customer_profiles WHERE batch_id=? GROUP BY segment ORDER BY segment
                """,batch.id());
    }
}
