package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/quantumwake/alethic-ism-core-go/pkg/data"
	"github.com/quantumwake/alethic-ism-core-go/pkg/model"
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
	natsRoute            *routing.NATSRoute // the route we are listening on
	natsRouteStateRouter *routing.NATSRoute // route for routing state messages
	natsRouteMonitor     *routing.NATSRoute // route for sending errors
	natsRouteStateSync   *routing.NATSRoute // route for sending sync messages
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

	// store the message in the message store and get the message back.
	// the first message is SourceData1 and the second will become SourceData2
	message, err := messageStore.StoreMessage(key, &queryState)
	if err != nil {
		handleError(routeMsg, context.Background(), err)
	}

	// both messages have been set then we can now merge and the messages together
	if message.SourceData1 != nil && message.SourceData2 != nil {

		//sendMonitorMessage(routeMsg.RouteID, data.Running, context.Background())

		// remove the message regardless of the outcome, TODO error handling and reporting needs to be improved
		defer messageStore.RemoveMessage(key)

		// merge the two messages together
		mergedSources, err := utils.ListDifferenceMerge(*message.SourceData1, *message.SourceData2)
		if err != nil {
			handleError(routeMsg, context.Background(), err)
		}

		// determine the processor id such that we can get the output state ids for the processor
		routeOn, err := dataAccess.FindRouteByID(routeMsg.RouteID)
		if err != nil {
			handleError(routeMsg, context.Background(), fmt.Errorf("error, no route found for route id %v, err: %v", routeMsg.RouteID, err))
		}

		// get the output states for the processor, for each output state we need to send the merged sources to its connected processor
		outputStateRoutes, err := dataAccess.FindRouteByProcessorAndDirection(routeOn.ProcessorID, model.DirectionOutput)
		if err != nil {
			handleError(routeMsg, context.Background(), fmt.Errorf("error, no output states found for current processor: %v", err))
			return
		}

		// TODO PERFORMANCE/CAPACITY/LOAD: we can probably batch these and have a send channel where we can batch the messages together
		for _, outputStateRoute := range outputStateRoutes {

			// each output state route will contain a state id, this state id is used to find which routes we need to send the merged sources to
			outputRoutes, err := dataAccess.FindRouteByStateAndDirection(outputStateRoute.StateID, model.DirectionInput)
			if err != nil {
				handleError(routeMsg, context.Background(), fmt.Errorf("error finding output routes for state id: %v, err: %v", outputStateRoute.StateID, err))
				continue
			}

			// we update the route between this processor and the output state id
			sendMonitorMessage(outputStateRoute.ID, data.Completed, context.Background())

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
					handleError(hopRouteMsg, context.Background(), fmt.Errorf("error publishing message: %v", err))
					continue
				}
			}

			// send the merged sources to the state router
			sendStateSyncMessage(outputStateRoute.ID, []map[string]interface{}{
				mergedSources,
			}, context.Background())
		}

	}
}

func onMessageReceived(ctx context.Context, route *routing.NATSRoute, msg *nats.Msg) {
	defer func(msg *nats.Msg, opts ...nats.AckOpt) {
		err := msg.Ack()
		if err != nil {
			log.Printf("error acking message: %v, error: %v", msg.Data, err)
		}
	}(msg)

	// unmarshal the message into a map object for processing the message data and metadata fields (e.g. binding)
	//var jsonMsg map[string]interface{}
	var routeMsg data.RouteMessage
	err := json.Unmarshal(msg.Data, &routeMsg)
	if err != nil {
		handleError(routeMsg, ctx, err)
		return
	}
	sendMonitorMessage(routeMsg.RouteID, data.Running, ctx)

	if routeMsg.QueryState == nil {
		handleError(routeMsg, ctx, fmt.Errorf("error: query state not found in message"))
		return
	}

	// iterate over the query state entries and process them
	for _, qse := range routeMsg.QueryState {
		handleQueryState(routeMsg, qse)
	}

	sendMonitorMessage(routeMsg.RouteID, data.Completed, ctx)
}

func sendMonitorMessage(routeID string, status data.ProcessorStatusCode, ctx context.Context) {
	monitorMessage := data.MonitorMessage{
		Type:    data.MonitorProcessorState,
		RouteID: routeID,
		Status:  status,
	}

	log.Printf("Sending monitor route: %v, status: %v\n", routeID, status)

	err := natsRouteMonitor.Publish(ctx, monitorMessage)
	if err != nil {
		// TODO need to log this error with proper error handling and logging
		log.Print("critical error: unable to publish error to monitor route")
	}

	err = natsRouteMonitor.Flush()
	if err != nil {
		// TODO need to log this error with proper error handling and logging
		log.Print("critical error: unable to publish error to monitor route")
	}
}

func sendStateSyncMessage(routeID string, queryState []map[string]interface{}, ctx context.Context) {
	syncMessage := data.RouteMessage{
		Type:       data.QueryStateRoute,
		RouteID:    routeID,
		QueryState: queryState,
	}

	log.Printf("Sending state to state sync route: %v\n", routeID)

	err := natsRouteStateSync.Publish(ctx, syncMessage)
	if err != nil {
		// TODO need to log this error with proper error handling and logging
		log.Print("critical error: unable to publish error to state sync route")
	}

	err = natsRouteStateSync.Flush()
	if err != nil {
		// TODO need to log this error with proper error handling and logging
		log.Print("critical error: unable to publish error to state sync route")
	}
}

func handleError(routeMsg data.RouteMessage, ctx context.Context, err error) {
	handleErrorWithParams(routeMsg.RouteID, routeMsg.QueryState, ctx, err)
}

func handleErrorWithParams(routeID string, dataValue interface{}, ctx context.Context, err error) {
	err = fmt.Errorf("error unmarshalling json object: %v", err)
	monitorMessage := data.MonitorMessage{
		Type:      data.MonitorProcessorState,
		RouteID:   routeID,
		Status:    data.Failed,
		Exception: err.Error(),
		Data:      dataValue,
	}

	err = natsRouteMonitor.Publish(ctx, monitorMessage)
	if err != nil {
		// TODO need to log this error with proper error handling and logging
		log.Print("Critical error: unable to publish error to monitor route")
	}
}

func main() {
	ctx, _ := context.WithCancel(context.Background())

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
	natsRouteStateRouter = routing.NewNATSRoute(stateRouter)

	// find monitor route, used for sending errors
	monitorRoute, err := routes.FindRouteBySelector("processor/monitor")
	if err != nil {
		log.Fatalf("error finding monitor route: %v", err)
	}

	// connect to monitor route
	natsRouteMonitor = routing.NewNATSRoute(monitorRoute)
	err = natsRouteMonitor.Connect(ctx)
	if err != nil {
		log.Fatalf("error connecting to monitor route: %v", err)
	}

	// connect to monitor route
	stateSyncRoute, err := routes.FindRouteBySelector("processor/state/sync")
	natsRouteStateSync = routing.NewNATSRoute(stateSyncRoute)
	err = natsRouteStateSync.Connect(ctx)
	if err != nil {
		log.Fatalf("error connecting to state sync route: %v", err)
	}

	// connect to usage database
	dsn, ok := os.LookupEnv("DSN")
	if !ok {
		log.Fatalf("DSN environment variable not set")
	}
	dataAccess = data.NewDataAccess(dsn)

	// Register a new Message Store and start the reconcile process in a new go routine
	messageStore = store.NewMessageStore()
	go messageStore.StartReconcile(ctx)
	//defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// create a new route and subscribe to inbound messages
	natsRoute = routing.NewNATSRouteWithCallback(route, onMessageReceived)
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
