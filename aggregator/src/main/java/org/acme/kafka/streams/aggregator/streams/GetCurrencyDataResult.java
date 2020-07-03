package org.acme.kafka.streams.aggregator.streams;

import org.acme.kafka.streams.aggregator.model.CurrencyData;

import java.util.Optional;

public class GetCurrencyDataResult {

    private static GetCurrencyDataResult NOT_FOUND = new GetCurrencyDataResult(null);

    private final CurrencyData result;

    private GetCurrencyDataResult(CurrencyData result){
        this.result = result;
    }

    public static GetCurrencyDataResult found(CurrencyData data) {
        return new GetCurrencyDataResult(data);
    }

    public static GetCurrencyDataResult notFound() {
        return NOT_FOUND;
    }

    public Optional<CurrencyData> getResult() {
        return Optional.ofNullable(result);
    }
}
