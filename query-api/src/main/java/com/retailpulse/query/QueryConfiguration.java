package com.retailpulse.query;

import com.clickhouse.jdbc.Driver;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.jdbc.DataSourceHealthIndicator;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.Properties;

@Configuration
public class QueryConfiguration {
    @Bean
    DataSource dataSource(@Value("${retailpulse.clickhouse.url}") String url,
                          @Value("${retailpulse.clickhouse.user}") String user,
                          @Value("${retailpulse.clickhouse.password}") String password) {
        if (password.isBlank()) throw new IllegalArgumentException("CLICKHOUSE_PASSWORD is required");
        var properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("connection_timeout", "2000");
        properties.setProperty("socket_timeout", "5000");
        properties.setProperty("retry", "0");
        properties.setProperty("max_execution_time", "5");
        var source = new SimpleDriverDataSource(new Driver(), url);
        source.setConnectionProperties(properties);
        return source;
    }

    @Bean
    DataSourceHealthIndicator dbHealthIndicator(DataSource source) {
        return new DataSourceHealthIndicator(source, "SELECT 1");
    }

    @Bean
    Jackson2ObjectMapperBuilderCustomizer exactNumbers() {
        return builder -> builder.serializerByType(BigDecimal.class, ToStringSerializer.instance)
                .serializerByType(Long.class, ToStringSerializer.instance)
                .serializerByType(Long.TYPE, ToStringSerializer.instance);
    }
}
