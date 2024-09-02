package main

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go"
	"github.com/quantumwake/alethic-ism-core-go/pkg/data"
	"github.com/quantumwake/alethic-ism-core-go/pkg/routing"
	"github.com/quantumwake/alethic-ism-transformer-composite/pkg/store"
	"github.com/quantumwake/alethic-ism-transformer-composite/pkg/utils"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	messageStore         *store.MessageStore
	natsRoute            *routing.NATSRoute
	natsRouteStateRouter *routing.NATSRoute
	dataAccess           *data.Access
)

func handleQueryState(routeMsg data.RouteMessage, queryState map[string]interface{}) {
	if queryState == nil {
		log.Printf("Error: query state not found in message")
		return
	}

	// we need to check the dictionary for the binding key and its value
	//value, ok := jsonMsg["__composite_key__"]
	compositeKey, ok := queryState["__composite_key__"]
	if !ok {
		// TODO send to monitor error
		log.Printf("Error: composition key not found in message")
		return // we need to return here because we cannot process the message without the binding key
	}
	key := compositeKey.(string)

	// store the message in the message store and get the message back
	// this should store the new message along with the previous message, if it exists
	// if both messages have been set then we can now merge and the messages together
	message, err := messageStore.StoreMessage(key, &queryState)
	if err != nil {
		log.Printf("error storing message: %v", err)
		// TODO submit error to to monitor route
		return
	}

	if message.SourceData1 != nil && message.SourceData2 != nil {
		// remove the message regardless of the outcome, TODO error handling and reporting needs to be improved
		defer messageStore.RemoveMessage(key)

		// merge the two messages together
		mergedSources, err := utils.ListDifferenceMerge(
			*message.SourceData1,
			*message.SourceData2)

		if err != nil {
			log.Printf("error merging sources: %v", err)
			// TODO error handling
			return
		}

		// determine the processor id
		routeOn, err := dataAccess.FindRouteByID(routeMsg.RouteID)
		if err != nil {
			log.Printf("error finding route by id: %v", routeMsg.RouteID)
			// TODO submit error to monitor route
			return
		}

		outputStates, err := dataAccess.FindRouteByProcessorAndDirection(routeOn.ProcessorID, data.DirectionOutput)
		if err != nil {
			log.Printf("error, no output states found for current processor: %v", err)
			// TODO submit error to monitor route
			return
		}

		// TODO PERFORMANCE/CAPACITY/LOAD: we can probably batch these and have a send channel where we can batch the messages together
		for _, outputState := range outputStates {
			// for each output state we need to send the merged sources to the connected processor as defined by state id and direction input
			outputRoutes, err := dataAccess.FindRouteByStateAndDirection(outputState.StateID, data.DirectionInput)
			if err != nil {
				log.Printf("error finding output routes: %v", err)
				// TODO submit error to monitor route
				continue
			}

			// now we iterate each of the state routes and send the merged sources to the connected processor
			for _, outputRoute := range outputRoutes {
				hopRouteMsg := data.RouteMessage{
					Type:       data.QueryStateEntry,
					RouteID:    outputRoute.ID,
					QueryState: []map[string]interface{}{mergedSources}, // TODO right now we only output one at a time, despite taking in multiple values
				}
				// TODO context.TODO() is a placeholder, we need to determine the context for the message
				err := natsRouteStateRouter.Publish(context.TODO(), hopRouteMsg)
				if err != nil {
					log.Printf("error publishing message: %v", err)
					// TODO submit error to monitor route
					continue
				}
			}
		}
	}
}

func onMessageReceived(route *routing.NATSRoute, msg *nats.Msg) {
	defer func(msg *nats.Msg, opts ...nats.AckOpt) {
		err := msg.Ack()
		if err != nil {

		}
	}(msg)

	// unmarshal the message into a map object for processing the message data and metadata fields (e.g. binding)
	//var jsonMsg map[string]interface{}
	var routeMsg data.RouteMessage
	err := json.Unmarshal(msg.Data, &routeMsg)
	if err != nil {
		// TODO send to monitor error
		log.Printf("Error unmarshalling json object: %v", err)
		return
	}

	if routeMsg.QueryState == nil {
		// TODO send to monitor error
		log.Printf("Error: query state not found in message")
		return
	}

	// iterate over the query state entries and process them
	for _, qse := range routeMsg.QueryState {
		handleQueryState(routeMsg, qse)
	}

}

func main() {
	// check routing file environment variable and set default if not found
	routingFile, ok := os.LookupEnv("ROUTING_FILE")
	if !ok {
		routingFile = "../routing-nats.yaml"
	}

	// load the nats routing table
	routes, err := routing.LoadConfig(routingFile)
	if err != nil {
		log.Fatal(err)
	}

	// find the usage route we want to listen on
	route, err := routes.FindRouteBySelector("data/transformers/mixer/state-composite-1.0")
	if err != nil {
		log.Fatalf("error finding coalescer route: %v", err)
	}

	// find the state router route
	stateRouter, err := routes.FindRouteBySelector("processor/state/router")
	if err != nil {
		log.Fatalf("error finding state router route: %v", err)
	}
	natsRouteStateRouter = routing.NewNATSRoute(stateRouter, onMessageReceived)

	// connect to usage database
	dsn, ok := os.LookupEnv("DSN")
	if !ok {
		log.Fatalf("DSN environment variable not set")
	}
	dataAccess = data.NewDataAccess(dsn)

	// Register a new Message Store and start the reconcile process in a new go routine
	messageStore = store.NewMessageStore()
	ctx, _ := context.WithCancel(context.Background())
	go messageStore.StartReconcile(ctx)
	//defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// create a new route and subscribe to inbound messages
	natsRoute = routing.NewNATSRoute(route, onMessageReceived)
	err = natsRoute.Subscribe(ctx)
	if err != nil {
		log.Fatalf("unable to subscribe to NATS: %v, route: %v", route, err)
	}

	<-sigChan
	log.Println("Received termination signal")
	err = natsRoute.Unsubscribe(ctx)
	if err != nil {
		return
	}
	//<-natsRoute.RunningDone

	log.Println("Gracefully shut down")

	// TODO need to have graceful shutdown here
	//select {}

	//// Wait for termination signal
	//<-sigChan
	//log.Println("Received termination signal")
	//
	//// Cancel the context to stop the reconcile loop
	//cancel()
	//
	//// Wait for the reconcile loop to finish
	//<-store.RunningDone
}
