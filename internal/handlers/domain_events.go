package handlers

import (
	"context"

	"github.com/v8tix/eda/am"
	"github.com/v8tix/eda/ddd"
	"github.com/v8tix/mallbots-customers-proto/pb"
	"github.com/v8tix/mallbots-customers/internal/domain"
)

type domainHandlers[T ddd.AggregateEvent] struct {
	publisher am.MessagePublisher[ddd.Event]
}

var _ ddd.EventHandler[ddd.AggregateEvent] = (*domainHandlers[ddd.AggregateEvent])(nil)

func NewDomainEventHandlers(publisher am.MessagePublisher[ddd.Event]) ddd.EventHandler[ddd.AggregateEvent] {
	return &domainHandlers[ddd.AggregateEvent]{
		publisher: publisher,
	}
}

func RegisterDomainEventHandlers(subscriber ddd.EventSubscriber[ddd.AggregateEvent], handlers ddd.EventHandler[ddd.AggregateEvent]) {
	subscriber.Subscribe(handlers,
		domain.CustomerRegisteredEvent,
		domain.CustomerSmsChangedEvent,
		domain.CustomerEnabledEvent,
		domain.CustomerDisabledEvent,
	)
}

func (h domainHandlers[T]) HandleEvent(ctx context.Context, event T) error {
	switch event.EventName() {
	case domain.CustomerRegisteredEvent:
		return h.onCustomerRegistered(ctx, event)
	case domain.CustomerSmsChangedEvent:
		return h.onCustomerSmsChanged(ctx, event)
	case domain.CustomerEnabledEvent:
		return h.onCustomerEnabled(ctx, event)
	case domain.CustomerDisabledEvent:
		return h.onCustomerDisabled(ctx, event)
	}
	return nil
}

func (h domainHandlers[T]) onCustomerRegistered(ctx context.Context, event ddd.AggregateEvent) error {
	payload := event.Payload().(*domain.CustomerRegistered)
	return h.publisher.Publish(ctx, pb.CustomerAggregateChannel,
		ddd.NewEvent(pb.CustomerRegisteredEvent, &pb.CustomerRegistered{
			Id:        payload.Customer.ID(),
			Name:      payload.Customer.Name,
			SmsNumber: payload.Customer.SmsNumber,
		}),
	)
}

func (h domainHandlers[T]) onCustomerSmsChanged(ctx context.Context, event ddd.AggregateEvent) error {
	payload := event.Payload().(*domain.CustomerRegistered)
	return h.publisher.Publish(ctx, pb.CustomerAggregateChannel,
		ddd.NewEvent(pb.CustomerSmsChangedEvent, &pb.CustomerSmsChanged{
			Id:        payload.Customer.ID(),
			SmsNumber: payload.Customer.SmsNumber,
		}),
	)
}

func (h domainHandlers[T]) onCustomerEnabled(ctx context.Context, event ddd.AggregateEvent) error {
	return h.publisher.Publish(ctx, pb.CustomerAggregateChannel,
		ddd.NewEvent(pb.CustomerEnabledEvent, &pb.CustomerEnabled{
			Id: event.AggregateID(),
		}),
	)
}

func (h domainHandlers[T]) onCustomerDisabled(ctx context.Context, event ddd.AggregateEvent) error {
	return h.publisher.Publish(ctx, pb.CustomerAggregateChannel,
		ddd.NewEvent(pb.CustomerDisabledEvent, &pb.CustomerDisabled{
			Id: event.AggregateID(),
		}),
	)
}
