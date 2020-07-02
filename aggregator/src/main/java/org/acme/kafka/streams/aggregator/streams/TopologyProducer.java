package org.acme.kafka.streams.aggregator.streams;

import io.quarkus.kafka.client.serialization.JsonbSerde;
import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.Currency;
import org.acme.kafka.streams.aggregator.model.ExchangeRate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.time.Instant;

@ApplicationScoped
public class TopologyProducer {

    static final String CURRENCY_EXCHANGE_STORE = "currency-exchange-store";
    private static final String CURRENCY_TOPIC = "currencies";
    private static final String EXCHANGE_RATE_TOPIC = "exchange-rates";
    private static final String CURRENCIES_AGGREGATED_TOPIC = "currencies-aggregated";

    @Produces
    public Topology buildTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        JsonbSerde<Currency> currencySerde = new JsonbSerde<>(Currency.class);
        JsonbSerde<Aggregation> aggregationSerde = new JsonbSerde<>(Aggregation.class);

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(CURRENCY_EXCHANGE_STORE);

        GlobalKTable<Integer, Currency> currencies = builder.globalTable(CURRENCY_TOPIC, Consumed.with(Serdes.Integer(),currencySerde ));

        builder.stream(EXCHANGE_RATE_TOPIC, Consumed.with(Serdes.Integer(),Serdes.String())).join(currencies, (currencyId,timestampAndValue) -> currencyId,
                (timestampAndValue,currency) -> {
                    String[] parts = timestampAndValue.split(";");
                    return new ExchangeRate(currency.id, currency.name, Instant.parse(parts[0]), Float.valueOf(parts[1]));
                }
                ).groupByKey()
                .aggregate(
                        Aggregation::new,
                        (currencyId, value, aggregation) -> aggregation.updateFrom(value),
                        Materialized.<Integer, Aggregation> as(storeSupplier)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(aggregationSerde)
                )
                .toStream()
                .to(
                        CURRENCIES_AGGREGATED_TOPIC,
                        Produced.with(Serdes.Integer(), aggregationSerde)
                );

        return builder.build();

    }

}
