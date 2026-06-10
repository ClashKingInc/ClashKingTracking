package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"clashking_tracking/internal/platform"

	valkey "github.com/valkey-io/valkey-go"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

const eventsDomainName = "events"

type eventsDomain struct{}

func NewEventsDomain() platform.Domain { return &eventsDomain{} }

func (d *eventsDomain) Name() string { return eventsDomainName }

func (d *eventsDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateEventsConfig(app.Config, app.Valkey); err != nil {
		return err
	}
	lis, err := net.Listen("tcp", app.Config.GRPCAddr)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	register(server, app.Valkey, app.Config)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	err = server.Serve(lis)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

func validateEventsConfig(cfg platform.Config, client valkey.Client) error {
	if client == nil {
		return errors.New("valkey_addr is required for events")
	}
	if cfg.EventStreamName == "" {
		return errors.New("events.stream is required")
	}
	if cfg.EventStreamGroup == "" {
		return errors.New("events.group is required")
	}
	if cfg.EventStreamConsumer == "" {
		return errors.New("events.consumer is required")
	}
	if cfg.EventStreamBatchSize <= 0 {
		return errors.New("events.batch_size must be greater than zero")
	}
	if cfg.EventStreamRetentionSeconds <= 0 {
		return errors.New("events.retention_seconds must be greater than zero")
	}
	if cfg.EventStreamReclaimIdleSeconds <= 0 {
		return errors.New("events.reclaim_idle_seconds must be greater than zero")
	}
	return nil
}

type eventService interface {
	Stream(*structpb.Struct, eventStreamServer) error
}

type eventStreamServer interface {
	Send(*structpb.Struct) error
	grpc.ServerStream
}

type service struct {
	client valkey.Client
	cfg    platform.Config
}

func register(server *grpc.Server, client valkey.Client, cfg platform.Config) {
	// The service is still registered manually so this script can expose a gRPC
	// stream without adding generated proto files to the tracking repo.
	svc := &service{client: client, cfg: cfg}
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
		filter := platform.Filter{Topics: toSet(req, "topics"), Clans: toSet(req, "clans")}
		if err := svc.ensureGroup(stream.Context()); err != nil {
			return err
		}
		for {
			if err := stream.Context().Err(); err != nil {
				return err
			}
			if err := svc.claimAndSend(stream.Context(), stream, filter); err != nil {
				return err
			}
			entries, err := svc.read(stream.Context(), ">")
			if err != nil {
				if valkey.IsValkeyNil(err) {
					continue
				}
				return err
			}
			if err := svc.sendEntries(stream.Context(), stream, filter, entries); err != nil {
				return err
			}
		}
	}
}

func (svc *service) ensureGroup(ctx context.Context) error {
	err := svc.client.Do(ctx, svc.client.B().XgroupCreate().
		Key(svc.cfg.EventStreamName).
		Group(svc.cfg.EventStreamGroup).
		Id("0").
		Mkstream().
		Build(),
	).Error()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (svc *service) claimAndSend(
	ctx context.Context,
	stream grpc.ServerStream,
	filter platform.Filter,
) error {
	entries, err := svc.autoClaim(ctx)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return svc.sendEntries(ctx, stream, filter, entries)
	}
	// A restart keeps this consumer's own pending entries in the PEL; reading ID
	// 0 replays those before blocking on new entries.
	entries, err = svc.read(ctx, "0")
	if err != nil && !valkey.IsValkeyNil(err) {
		return err
	}
	return svc.sendEntries(ctx, stream, filter, entries)
}

func (svc *service) read(ctx context.Context, id string) ([]valkey.XRangeEntry, error) {
	result, err := svc.client.Do(ctx, svc.client.B().Xreadgroup().
		Group(svc.cfg.EventStreamGroup, svc.cfg.EventStreamConsumer).
		Count(int64(svc.cfg.EventStreamBatchSize)).
		Block(5000).
		Streams().
		Key(svc.cfg.EventStreamName).
		Id(id).
		Build(),
	).AsXRead()
	if err != nil {
		return nil, err
	}
	return result[svc.cfg.EventStreamName], nil
}

func (svc *service) autoClaim(ctx context.Context) ([]valkey.XRangeEntry, error) {
	minIdle := fmt.Sprintf("%d", svc.cfg.EventStreamReclaimIdleSeconds*1000)
	values, err := svc.client.Do(ctx, svc.client.B().Xautoclaim().
		Key(svc.cfg.EventStreamName).
		Group(svc.cfg.EventStreamGroup).
		Consumer(svc.cfg.EventStreamConsumer).
		MinIdleTime(minIdle).
		Start("0-0").
		Count(int64(svc.cfg.EventStreamBatchSize)).
		Build(),
	).ToArray()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(values) < 2 {
		return nil, nil
	}
	return values[1].AsXRange()
}

func (svc *service) sendEntries(
	ctx context.Context,
	stream grpc.ServerStream,
	filter platform.Filter,
	entries []valkey.XRangeEntry,
) error {
	for _, entry := range entries {
		event, ok := streamEntryEvent(entry)
		if !ok {
			if err := svc.ack(ctx, entry.ID); err != nil {
				return err
			}
			continue
		}
		if !eventMatches(filter, event) {
			if err := svc.ack(ctx, entry.ID); err != nil {
				return err
			}
			continue
		}
		payload, err := structpb.NewStruct(map[string]any{
			"topic":     event.Topic,
			"clan_tag":  event.ClanTag,
			"timestamp": event.Timestamp.Format(time.RFC3339Nano),
			"value":     event.Value,
		})
		if err != nil {
			continue
		}
		if err := stream.SendMsg(payload); err != nil {
			return err
		}
		if err := svc.ack(ctx, entry.ID); err != nil {
			return err
		}
	}
	return nil
}

func (svc *service) ack(ctx context.Context, id string) error {
	return svc.client.Do(ctx, svc.client.B().Xack().
		Key(svc.cfg.EventStreamName).
		Group(svc.cfg.EventStreamGroup).
		Id(id).
		Build(),
	).Error()
}

func streamEntryEvent(entry valkey.XRangeEntry) (platform.Event, bool) {
	timestamp, _ := time.Parse(time.RFC3339Nano, entry.FieldValues["timestamp"])
	var value map[string]any
	if err := json.Unmarshal([]byte(entry.FieldValues["value"]), &value); err != nil {
		return platform.Event{}, false
	}
	return platform.Event{
		Topic:     entry.FieldValues["topic"],
		ClanTag:   entry.FieldValues["clan_tag"],
		Timestamp: timestamp,
		Value:     value,
	}, true
}

func eventMatches(filter platform.Filter, event platform.Event) bool {
	if len(filter.Topics) > 0 {
		if _, ok := filter.Topics[event.Topic]; !ok {
			return false
		}
	}
	if len(filter.Clans) > 0 {
		if _, ok := filter.Clans[event.ClanTag]; !ok {
			return false
		}
	}
	return true
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
