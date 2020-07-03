package org.acme.kafka.streams.aggregator.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class CurrencyData {
    public int currencyId;
    public String currencyName;
    public double min = Double.MAX_VALUE;
    public double max = Double.MIN_VALUE;
    public int count;
    public double avg;

    public CurrencyData(int currencyId, String currencyName, double min, double max, int count, double avg) {
        this.currencyId = currencyId;
        this.currencyName = currencyName;
        this.min = min;
        this.max = max;
        this.count = count;
        this.avg = avg;
    }

    public static CurrencyData from(Aggregation aggregation){
        return new CurrencyData(aggregation.currencyId,aggregation.currencyName, aggregation.min, aggregation.max, aggregation.count,aggregation.avg);
    }
}
