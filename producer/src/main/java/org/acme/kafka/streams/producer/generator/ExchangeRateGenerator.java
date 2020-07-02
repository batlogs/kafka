package org.acme.kafka.streams.producer.generator;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ApplicationScoped
public class ExchangeRateGenerator {
    private static final Logger LOG = Logger.getLogger(ExchangeRateGenerator.class);

    private Random random = new Random();

    private List<Currency> currencies = Collections.unmodifiableList(Arrays.asList(
            new Currency(1, "USD", 13),
            new Currency(2, "GBP", 5),
            new Currency(3, "AUD", 11),
            new Currency(4, "SUD", 16),
            new Currency(5, "YEN", 12),
            new Currency(6, "EURO", 7),
            new Currency(7, "CAND", 11),
            new Currency(8, "RUB", 7),
            new Currency(9, "YUAN", 20)
    ));

    @Outgoing("currency-values")
    public Flowable<KafkaRecord<Integer, String>>generate(){
        return Flowable.interval(500, TimeUnit.MILLISECONDS).onBackpressureDrop().map(tick ->{
            Currency currency = currencies.get(random.nextInt(currencies.size()));
            float exchangeRate = BigDecimal.valueOf(random.nextGaussian()*15+currency.exchangeRate).setScale(1, RoundingMode.HALF_UP).floatValue();
            LOG.infov("",currency.name,exchangeRate);
            return KafkaRecord.of(currency.id, Instant.now() +";"+exchangeRate);
        });
    }

    @Outgoing("currency-exchange")
    public Flowable<KafkaRecord<Integer, String>> currencyExchange() {
        List<KafkaRecord<Integer, String>> stationsAsJson = currencies.stream()
                .map(s -> KafkaRecord.of(
                        s.id,
                        "{ \"id\" : " + s.id +
                                ", \"name\" : \"" + s.name + "\" }"))
                .collect(Collectors.toList());

        return Flowable.fromIterable(stationsAsJson);
    };

    private static class Currency{
        int id;
        String name;
        int exchangeRate;

        public Currency(int id, String name, int exchangeRate) {
            this.id = id;
            this.name = name;
            this.exchangeRate = exchangeRate;
        }
    }
}

