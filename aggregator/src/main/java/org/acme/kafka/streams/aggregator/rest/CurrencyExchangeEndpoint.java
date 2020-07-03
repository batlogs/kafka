package org.acme.kafka.streams.aggregator.rest;

import org.acme.kafka.streams.aggregator.streams.GetCurrencyDataResult;
import org.acme.kafka.streams.aggregator.streams.InteractiveQueries;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@ApplicationScoped
@Path("/currency-exchange")
public class CurrencyExchangeEndpoint {

    @Inject
    InteractiveQueries interactiveQueries;

    @GET
    @Path("/data/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getWeatherStationData(@PathParam("id") int id) {
        GetCurrencyDataResult result = interactiveQueries.geCurrencyData(id);

        if (result.getResult().isPresent()) {
            return Response.ok(result.getResult().get()).build();
        }
        else {
            return Response.status(Response.Status.NOT_FOUND.getStatusCode(),
                    "No data found for currency " + id).build();
        }
    }

}

