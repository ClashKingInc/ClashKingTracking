package scripts

import (
	"context"
	"net"
	"time"

	"clashking_tracking/internal/platform"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

const eventsDomainName = "events"

type eventsDomain struct{}

func NewEventsDomain() platform.Domain { return &eventsDomain{} }

func (d *eventsDomain) Name() string { return eventsDomainName }

func (d *eventsDomain) Run(ctx context.Context, app *platform.App) error {
	lis, err := net.Listen("tcp", app.Config.GRPCAddr)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	register(server, app.Bus)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	return server.Serve(lis)
}

type eventService interface {
	Stream(*structpb.Struct, eventStreamServer) error
}

type eventStreamServer interface {
	Send(*structpb.Struct) error
	grpc.ServerStream
}

type service struct {
	bus *platform.Bus
}

func register(server *grpc.Server, bus *platform.Bus) {
	// The service is registered manually so the runtime can expose gRPC streaming
	// without introducing a generated proto package into the script layer.
	svc := &service{bus: bus}
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "tracking.events.v1.EventService",
		HandlerType: (*eventService)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "Stream",
			Handler:       streamHandler(svc),
			ServerStreams: true,
		}},
	}, svc)
}

func streamHandler(svc *service) grpc.StreamHandler {
	return func(_ any, stream grpc.ServerStream) error {
		req := &structpb.Struct{}
		if err := stream.RecvMsg(req); err != nil {
			return err
		}
		// Convert the ad hoc request payload into the bus filter once, then keep
		// the stream hot until the subscriber or caller closes it.
		filter := platform.Filter{Topics: toSet(req, "topics"), Clans: toSet(req, "clans")}
		events, cancel := svc.bus.Subscribe(filter)
		defer cancel()
		for {
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case event, ok := <-events:
				if !ok {
					return nil
				}
				payload, err := structpb.NewStruct(map[string]any{
					"topic":     event.Topic,
					"clan_tag":  event.ClanTag,
					"timestamp": event.Timestamp.Format(time.RFC3339),
					"value":     event.Value,
				})
				if err != nil {
					continue
				}
				if err := stream.SendMsg(payload); err != nil {
					return err
				}
			}
		}
	}
}

func (s *service) Stream(*structpb.Struct, eventStreamServer) error { return nil }

func toSet(input *structpb.Struct, field string) map[string]struct{} {
	out := make(map[string]struct{})
	value, ok := input.Fields[field]
	if !ok || value.GetListValue() == nil {
		return out
	}
	for _, item := range value.GetListValue().Values {
		if text := item.GetStringValue(); text != "" {
			out[text] = struct{}{}
		}
	}
	return out
}
