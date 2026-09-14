package com.retailpulse.query;

import com.retailpulse.customer.ProfileStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
@ConditionalOnProperty(name="retailpulse.customer.enabled",havingValue="true")
public class CustomerConfiguration {
    @Bean ProfileStore profileStore(@Value("${retailpulse.customer.url}") String url,
                                   @Value("${retailpulse.customer.password}") String password) {
        if(password.isBlank()) throw new IllegalArgumentException("CUSTOMER_MYSQL_PASSWORD is required");
        return new ProfileStore(new DriverManagerDataSource(url,"customer",password));
    }
}
