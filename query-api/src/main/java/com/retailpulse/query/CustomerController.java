package com.retailpulse.query;

import com.retailpulse.customer.Profile;
import com.retailpulse.customer.ProfileStore;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/customers")
@ConditionalOnProperty(name="retailpulse.customer.enabled",havingValue="true")
public class CustomerController {
    private final ProfileStore store;
    public CustomerController(ProfileStore store) {this.store=store;}

    @GetMapping public ProfileStore.Page list(
            @RequestParam(defaultValue="uci-online-retail") @NotBlank @Size(max=64) String dataset,
            @RequestParam(required=false) @Size(max=64) String batch,
            @RequestParam(required=false) @Size(max=64) String after,
            @RequestParam(required=false) @Pattern(regexp="SINGLE_PURCHASE|AT_RISK|HIGH_VALUE|REPEAT_CUSTOMER") String segment,
            @RequestParam(required=false) @Size(max=64) String customerId,
            @RequestParam(defaultValue="25") @Min(1) @Max(100) int limit) {
        return store.list(dataset,batch,after,segment,customerId,limit);
    }

    @GetMapping("/{customerId}") public ResponseEntity<Profile> detail(
            @PathVariable @Size(max=64) String customerId,
            @RequestParam(defaultValue="uci-online-retail") @NotBlank @Size(max=64) String dataset,
            @RequestParam @NotBlank @Size(max=64) String batch) {
        return store.find(dataset,batch,customerId).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{customerId}/prediction") public ResponseEntity<ProfileStore.Prediction> prediction(
            @PathVariable @Size(max=64) String customerId,
            @RequestParam(defaultValue="uci-online-retail") @NotBlank @Size(max=64) String dataset,
            @RequestParam @NotBlank @Size(max=64) String batch) {
        return store.prediction(dataset,batch,customerId).map(ResponseEntity::ok).orElseGet(()->ResponseEntity.noContent().build());
    }
    @GetMapping("/summary") public List<Map<String,Object>> summary(
            @RequestParam(defaultValue="uci-online-retail") @NotBlank @Size(max=64) String dataset,
            @RequestParam @NotBlank @Size(max=64) String batch) {
        return store.summary(dataset,batch);
    }
}
