package org.acme.kafka.streams.aggregator.model;

import java.time.Instant;

public class ExchangeRate {
    public int currencyId;
    public String currencyName;
    public Instant timestamp;
    public float rate;

    public ExchangeRate(int currencyId, String currencyName, Instant timestamp, float rate) {
        this.currencyId = currencyId;
        this.currencyName = currencyName;
        this.timestamp = timestamp;
        this.rate = rate;
    }
}
