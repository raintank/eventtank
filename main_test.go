package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bmizerany/assert"
	"github.com/codeskyblue/go-uuid"
	"github.com/raintank/eventtank/eventdef"
	"gopkg.in/raintank/schema.v1"
)

func makeEvent(timestamp time.Time) *schema.ProbeEvent {
	e := new(schema.ProbeEvent)
	e.Id = uuid.NewUUID().String()
	e.EventType = "monitor_state"
	e.OrgId = 1
	e.Severity = "ERROR"
	e.Source = "network_collector"
	e.Message = "100% packet loss"
	e.Timestamp = timestamp.UnixNano() / int64(time.Millisecond)

	return e
}

func TestAddInProgressMessage(t *testing.T) {
	e := makeEvent(time.Now())

	writeQueue = NewInProgressMessageQueue()
	go writeQueue.Loop()

	writeQueue.EnQueue(&inProgressMessage{
		Timestamp: time.Now(),
		Event:     e,
		Message: &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 1,
			Offset:    10,
		},
	})

	assert.T(t, len(writeQueue.inProgress) == 1, fmt.Sprintf("expected 1 item in inProgress queue. found %d", len(writeQueue.inProgress)))
}

func TestAddInProgressMessageThenProcess(t *testing.T) {
	e := makeEvent(time.Now())

	writeQueue = NewInProgressMessageQueue()
	go writeQueue.Loop()

	writeQueue.EnQueue(&inProgressMessage{
		Timestamp: time.Now(),
		Event:     e,
		Message: &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 1,
			Offset:    10,
		},
	})

	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 1, fmt.Sprintf("expected 1 item in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()

	//push a saveStatus to the status Channel
	writeQueue.status <- []*eventdef.BulkSaveStatus{{
		Id: e.Id,
		Ok: true,
	}}

	time.Sleep(time.Millisecond)
	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 0, fmt.Sprintf("expected 0 items in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()
}

func TestAddInProgressMessageThenProcessFailed(t *testing.T) {
	e := makeEvent(time.Now())

	writeQueue = NewInProgressMessageQueue()
	go writeQueue.Loop()

	writeQueue.EnQueue(&inProgressMessage{
		Timestamp: time.Now(),
		Event:     e,
		Message: &sarama.ConsumerMessage{
			Topic:     "test",
			Partition: 1,
			Offset:    10,
		},
	})

	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 1, fmt.Sprintf("expected 1 item in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()

	//push a saveStatus to the status Channel
	writeQueue.status <- []*eventdef.BulkSaveStatus{{
		Id: e.Id,
		Ok: false,
	}}

	time.Sleep(time.Millisecond)
	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 1, fmt.Sprintf("expected 1 items in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()
}

func TestAddMultipleInProgressMessageThenProcess(t *testing.T) {
	writeQueue = NewInProgressMessageQueue()
	go writeQueue.Loop()

	events := make([]*schema.ProbeEvent, 100)
	for i := 0; i < 100; i++ {
		e := makeEvent(time.Now())
		events[i] = e
		go writeQueue.EnQueue(&inProgressMessage{
			Timestamp: time.Now(),
			Event:     e,
			Message: &sarama.ConsumerMessage{
				Topic:     "test",
				Partition: 1,
				Offset:    int64(i),
			},
		})
	}
	time.Sleep(time.Millisecond)

	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 100, fmt.Sprintf("expected 100 item in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()
	statuses := make([]*eventdef.BulkSaveStatus, 0)
	for _, e := range events {
		//push a saveStatus to the status Channel
		statuses = append(statuses, &eventdef.BulkSaveStatus{
			Id: e.Id,
			Ok: true,
		})
	}
	writeQueue.status <- statuses

	time.Sleep(time.Second)
	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 0, fmt.Sprintf("expected 0 items in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()
}
