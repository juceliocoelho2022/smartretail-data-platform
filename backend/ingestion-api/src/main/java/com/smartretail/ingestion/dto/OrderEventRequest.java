package com.smartretail.ingestion.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;

public record OrderEventRequest(
        @NotBlank @Size(max = 80) String customerId,
        @NotBlank @Size(max = 80) String productId,
        @NotNull @Min(1) Integer quantity,
        @NotNull @DecimalMin("0.01") BigDecimal unitPrice,
        @NotBlank @Size(max = 30) String channel,
        @NotBlank @Size(max = 80) String location
) {}
