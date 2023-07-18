package com.batch.domain;

import java.math.BigDecimal;
import java.util.Date;

public record Transaction(String account, Date timestamp, BigDecimal amount) {
}
