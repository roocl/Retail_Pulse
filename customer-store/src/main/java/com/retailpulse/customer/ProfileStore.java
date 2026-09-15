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
    private final BatchPublisher publisher;
    private static final RowMapper<Profile> PROFILE = (r,n) -> new Profile(r.getString("customer_id"),r.getString("country"),
            r.getInt("recency_days"),r.getLong("orders"),r.getBigDecimal("purchase_amount"),r.getBigDecimal("cancellation_amount"),
            r.getString("preferred_product"),r.getInt("r_score"),r.getInt("f_score"),r.getInt("m_score"),r.getString("segment"));
    private static final RowMapper<ProfileBatch> BATCH = (r,n) -> new ProfileBatch(r.getString("id"),r.getString("dataset"),
            r.getString("source_release"),r.getString("rule_version"),r.getDate("observation").toLocalDate(),r.getInt("window_days"));

    public ProfileStore(DataSource source) {
        this.source=source;
        jdbc=new JdbcTemplate(source);
        jdbc.setQueryTimeout(15);
        publisher=new BatchPublisher(jdbc,new TransactionTemplate(new DataSourceTransactionManager(source)));
    }

    public void initialize() {
        new ResourceDatabasePopulator(new ClassPathResource("customer/schema.sql")).execute(source);
    }

    public void publish(ProfileBatch batch, List<Profile> profiles) {
        var sorted=profiles.stream().sorted(Comparator.comparing(Profile::customerId)).toList();
        String hash=contentHash(batch,sorted);
        publisher.publish(BatchPublisher.Kind.PROFILE,batch.dataset(),batch.id(),hash,()->{
            jdbc.update("INSERT INTO profile_batches(id,dataset,source_release,rule_version,observation,window_days,content_hash,customer_count) VALUES (?,?,?,?,?,?,?,?)",
                    batch.id(),batch.dataset(),batch.sourceRelease(),batch.ruleVersion(),batch.observation(),batch.windowDays(),hash,sorted.size());
            jdbc.batchUpdate("INSERT INTO customer_profiles VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",sorted,500,(statement,p) -> {
                statement.setString(1,batch.id());statement.setString(2,p.customerId());statement.setString(3,p.country());
                statement.setInt(4,p.recencyDays());statement.setLong(5,p.orders());statement.setBigDecimal(6,p.purchaseAmount());
                statement.setBigDecimal(7,p.cancellationAmount());statement.setString(8,p.preferredProduct());
                statement.setInt(9,p.rScore());statement.setInt(10,p.fScore());statement.setInt(11,p.mScore());statement.setString(12,p.segment());
            });
        },()->{
            if(!jdbc.query("SELECT * FROM customer_profiles WHERE batch_id=? ORDER BY customer_id",PROFILE,batch.id()).equals(sorted))
                throw new IllegalStateException("Published customer data is inconsistent");
        });
    }

    private static String contentHash(Object batch,List<?> rows) {
        try {
            var digest=MessageDigest.getInstance("SHA-256");
            digest.update(batch.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest.digest(new ObjectMapper().writeValueAsBytes(rows)));
        } catch(Exception error){throw new IllegalStateException("Cannot identify batch content",error);}
    }

    public record Prediction(ScoreBatch batch,CustomerScore score) {}

    public void publishScores(ScoreBatch batch,List<CustomerScore> scores) {
        var sorted=scores.stream().sorted(Comparator.comparing(CustomerScore::customerId)).toList();
        publisher.publish(BatchPublisher.Kind.SCORE,batch.profileBatchId(),batch.id(),contentHash(batch,sorted),()->{
            jdbc.update("INSERT INTO score_batches(id,profile_batch_id,model_id,algorithm,horizon_days,content_hash,customer_count) VALUES (?,?,?,?,?,?,?)",
                    batch.id(),batch.profileBatchId(),batch.modelId(),batch.algorithm(),batch.horizonDays(),contentHash(batch,sorted),sorted.size());
            jdbc.batchUpdate("INSERT INTO customer_scores(batch_id,profile_batch_id,customer_id,score) VALUES (?,?,?,?)",sorted,500,(statement,row)->{
                statement.setString(1,batch.id());statement.setString(2,batch.profileBatchId());
                statement.setString(3,row.customerId());statement.setBigDecimal(4,row.value());
            });
        },()->{
            var expected=jdbc.queryForList("SELECT customer_id FROM customer_profiles WHERE batch_id=? ORDER BY customer_id",String.class,batch.profileBatchId());
            if(!expected.equals(sorted.stream().map(CustomerScore::customerId).toList()))
                throw new IllegalStateException("Scores must cover exactly the profile batch customers");
            var actual=jdbc.query("SELECT customer_id,score FROM customer_scores WHERE batch_id=? ORDER BY customer_id",
                    (r,n)->new CustomerScore(r.getString(1),r.getBigDecimal(2)),batch.id());
            if(!actual.equals(sorted))throw new IllegalStateException("Published score data is inconsistent");
        });
    }

    public Optional<Prediction> prediction(String dataset,String profileBatchId,String customerId) {
        return jdbc.query("""
                SELECT b.*,s.customer_id,s.score FROM score_current c
                JOIN score_batches b ON b.id=c.batch_id
                JOIN customer_scores s ON s.batch_id=b.id
                JOIN profile_batches p ON p.id=c.profile_batch_id
                WHERE p.dataset=? AND p.id=? AND s.customer_id=?
                """,(r,n)->new Prediction(new ScoreBatch(r.getString("id"),r.getString("profile_batch_id"),
                        r.getString("model_id"),r.getString("algorithm"),r.getInt("horizon_days")),
                        new CustomerScore(r.getString("customer_id"),r.getBigDecimal("score"))),dataset,profileBatchId,customerId).stream().findFirst();
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
