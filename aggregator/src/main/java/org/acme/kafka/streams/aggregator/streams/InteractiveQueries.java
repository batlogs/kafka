package org.acme.kafka.streams.aggregator.streams;

import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.CurrencyData;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class InteractiveQueries {
    @Inject
    KafkaStreams streams;
    public GetCurrencyDataResult geCurrencyData(int id) {
        Aggregation result = getCurrencyExchangeStore().get(id);

        if (result != null) {
            return GetCurrencyDataResult.found(CurrencyData.from(result));
        }
        else {
            return GetCurrencyDataResult.notFound();
        }
    }

    private ReadOnlyKeyValueStore<Integer, Aggregation> getCurrencyExchangeStore() {
        while (true) {
            try {
                return streams.store(TopologyProducer.CURRENCY_EXCHANGE_STORE, QueryableStoreTypes.keyValueStore());
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}
