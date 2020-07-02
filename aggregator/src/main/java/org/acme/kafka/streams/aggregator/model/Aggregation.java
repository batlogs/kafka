package org.acme.kafka.streams.aggregator.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.math.BigDecimal;
import java.math.RoundingMode;

@RegisterForReflection
public class Aggregation {

    public int currencyId;
    public String currencyName;
    public double min = Double.MAX_VALUE;
    public double max = Double.MIN_VALUE;
    public int count;
    public double sum;
    public double avg;

    public Aggregation updateFrom(ExchangeRate exchangeRate){
        currencyId = exchangeRate.currencyId;
        currencyName = exchangeRate.currencyName;

        count++;
        sum += exchangeRate.rate;
        avg = BigDecimal.valueOf(sum / count)
                .setScale(1, RoundingMode.HALF_UP).doubleValue();
        min = Math.min(min, exchangeRate.rate);
        max = Math.max(max, exchangeRate.rate);

        return this;

    }
}
