package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"

	"github.com/codeskyblue/go-uuid"
	"github.com/raintank/eventtank/eventdef"
	"github.com/rakyll/globalconf"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topicStr                  = flag.String("topic", "probe_events", "Kafka topic (may be given multiple times as a comma-separated list)")
	group                     = flag.String("group", "eventtank", "Kafka consumer group")
	brokerStr                 = flag.String("brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	channelBufferSize         = flag.Int("channel-buffer-size", 1000, "The number of metrics to buffer in internal and external channels")
	consumerFetchMin          = flag.Int("consumer-fetch-min", 102400, "The minimum number of message bytes to fetch in a request")
	consumerFetchDefault      = flag.Int("consumer-fetch-default", 1024000, "The default number of message bytes to fetch in a request")
	consumerMaxWaitTime       = flag.String("consumer-max-wait-time", "1s", "The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyway")
	consumerMaxProcessingTime = flag.String("consumer-max-processing-time", "1s", "The maximum amount of time the consumer expects a message takes to process")

	esAddr      = flag.String("elastic-addr", "localhost:9200", "elasticsearch address (default: localhost:9200)")
	esBatchSize = flag.Int("elastic-batch-size", 1000, "maximum number of events in each bulkIndex request")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address (default: localhost:8125)")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog (default: standard)")
	confFile   = flag.String("config", "/etc/raintank/eventtank.ini", "configuration file (default /etc/raintank/eventtank.ini")

	logLevel   = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	listenAddr = flag.String("listen", ":6060", "http listener address.")

	eventsToEsOK   met.Count
	eventsToEsFail met.Count
	esPutDuration  met.Timer
	messagesSize   met.Meter
	msgsAge        met.Meter // in ms
	msgsHandleOK   met.Count
	msgsHandleFail met.Count

	writeQueue *InProgressMessageQueue
	GitHash    = "(none)"

	consumer *cluster.Consumer
)

func Consume(done chan struct{}) {
	for m := range consumer.Messages() {
		if *logLevel < 2 {
			log.Debug("received message: Topic %s, Partition: %d, Offset: %d, Key: %x", m.Topic, m.Partition, m.Offset, m.Key)
		}
		ms, err := msg.ProbeEventFromMsg(m.Value)
		if err != nil {
			log.Error(3, "skipping message. %s", err)
			continue
		}
		messagesSize.Value(int64(len(m.Value)))
		msgsAge.Value(time.Now().Sub(ms.Produced).Nanoseconds() / 1000)
		err = ms.DecodeProbeEvent()
		if err != nil {
			log.Error(3, "skipping message. %s", err)
			continue
		}
		if ms.Event.Id == "" {
			// per http://blog.mikemccandless.com/2014/05/choosing-fast-unique-identifier-uuid.html,
			// using V1 UUIDs is much faster than v4 like we were using
			u := uuid.NewUUID()
			ms.Event.Id = u.String()
		}
		if ms.Event.Timestamp == 0 {
			// looks like this expects timestamps in milliseconds
			ms.Event.Timestamp = ms.Produced.UnixNano() / int64(time.Millisecond)
		}
		if err := ms.Event.Validate(); err != nil {
			e, _ := json.Marshal(ms.Event)
			log.Error(3, "Skipping Invalid event payload: %s", string(e))
			continue
		}
		inProgress := &inProgressMessage{
			Timestamp: time.Now(),
			Message:   m,
			Event:     ms.Event,
		}
		writeQueue.ProcessChan <- inProgress
	}
	close(done)
}

type inProgressMessage struct {
	Timestamp time.Time
	Message   *sarama.ConsumerMessage
	Event     *schema.ProbeEvent
	saved     bool
}

type InProgressMessageQueue struct {
	sync.RWMutex
	inProgress  map[string]*inProgressMessage
	status      chan []*eventdef.BulkSaveStatus
	ProcessChan chan *inProgressMessage
}

func (q *InProgressMessageQueue) ProcessInProgress() {
	for in := range q.ProcessChan {
		writeQueue.EnQueue(in)
		saved := false
		failCount := 0
		for !saved {
			if err := eventdef.Save(in.Event); err != nil {
				log.Error(3, "couldn't process %s: %s", in.Event.Id, err)
				if failCount == 0 {
					msgsHandleFail.Inc(1)
				}
				failCount++
				if failCount > 10 {
					log.Fatal(4, "Unable to add events to the bulkindexer for 10seconds.  Terminating process.")
				}
				time.Sleep(time.Second)
			} else {
				saved = true
			}
		}
	}
}

func (q *InProgressMessageQueue) EnQueue(m *inProgressMessage) {
	q.Lock()
	q.inProgress[m.Event.Id] = m
	q.Unlock()
}

// check all outstanding events to find offsets that can be marked as processed.
// Each kafka paritions is just a log, so we can only mark offests up the oldest offset
// that is still being processed.  eg. if offset 20 is saved to ES, but offset 5 is still
// pending, then we can only mark up to offset 4.
func (q *InProgressMessageQueue) markOffsets() {
	savedOffests := make(map[string]map[int32]map[int]string)
	unsavedOffest := make(map[string]map[int32][]int)
	for eventId, msg := range q.inProgress {
		if msg.saved {
			if _, ok := savedOffests[msg.Message.Topic]; !ok {
				savedOffests[msg.Message.Topic] = make(map[int32]map[int]string)
			}
			if _, ok := savedOffests[msg.Message.Topic][msg.Message.Partition]; !ok {
				savedOffests[msg.Message.Topic][msg.Message.Partition] = make(map[int]string)
			}
			savedOffests[msg.Message.Topic][msg.Message.Partition][int(msg.Message.Offset)] = eventId

		} else {
			if _, ok := unsavedOffest[msg.Message.Topic]; !ok {
				unsavedOffest[msg.Message.Topic] = make(map[int32][]int)
			}
			if _, ok := unsavedOffest[msg.Message.Topic][msg.Message.Partition]; !ok {
				unsavedOffest[msg.Message.Topic][msg.Message.Partition] = make([]int, 0)
			}
			unsavedOffest[msg.Message.Topic][msg.Message.Partition] = append(unsavedOffest[msg.Message.Topic][msg.Message.Partition], int(msg.Message.Offset))
		}
	}
	for topic := range savedOffests {
		if _, ok := unsavedOffest[topic]; !ok {
			// no unsaved offests for this topic.
			for partition := range savedOffests[topic] {
				newestOffset := 0
				for offset, id := range savedOffests[topic][partition] {
					if offset > newestOffset {
						newestOffset = offset
					}
					delete(q.inProgress, id)
				}
				if consumer != nil {
					consumer.MarkPartitionOffset(topic, partition, int64(newestOffset), "")
				}
			}
			continue
		}
		for partition := range savedOffests[topic] {
			if _, ok := unsavedOffest[topic][partition]; ok {
				// no unsaved offests for this partition.
				newestOffset := 0
				for offset, id := range savedOffests[topic][partition] {
					if offset > newestOffset {
						newestOffset = offset
					}
					delete(q.inProgress, id)
				}
				if consumer != nil {
					consumer.MarkPartitionOffset(topic, partition, int64(newestOffset), "")
				}
				continue
			}

			sort.IntSlice(unsavedOffest[topic][partition]).Sort()
			oldestUnsaved := unsavedOffest[topic][partition][0]

			offsets := make([]int, len(savedOffests[topic][partition]))
			i := 0
			for o := range savedOffests[topic][partition] {
				offsets[i] = o
				i++
			}
			sort.IntSlice(offsets).Sort()
			// start at the end of the list of saved offests and work backwards.
			// the first offest less then the oldestUnsaved offset is the offset that we need to mark.
			found := false
			for i := len(offsets) - 1; i >= 0; i-- {
				if found {
					delete(q.inProgress, savedOffests[topic][partition][offsets[i]])
				}
				if offsets[i] < oldestUnsaved {
					if consumer != nil {
						consumer.MarkPartitionOffset(topic, partition, int64(offsets[i]), "")
					}
					found = true
					delete(q.inProgress, savedOffests[topic][partition][offsets[i]])
				}
			}
		}
	}
}

func (q *InProgressMessageQueue) Loop() {
	for statuses := range q.status {
		q.Lock()
		for _, s := range statuses {
			if m, ok := q.inProgress[s.Id]; ok {
				if s.Ok {
					m.saved = true
					eventsToEsOK.Inc(1)
					msgsHandleOK.Inc(1)
					log.Debug("event %s commited to ES", s.Id)
				} else {
					eventsToEsFail.Inc(1)
					msgsHandleFail.Inc(1)
					log.Error(3, "event %s failed to save, requeueing", s.Id)
					q.ProcessChan <- m
				}
				esPutDuration.Value(time.Now().Sub(m.Timestamp))
			} else {
				log.Error(3, "got processing response for unknown message. event %s", s.Id)
			}
		}
		q.markOffsets()
		q.Unlock()
	}
}

func NewInProgressMessageQueue() *InProgressMessageQueue {
	q := &InProgressMessageQueue{
		inProgress:  make(map[string]*inProgressMessage),
		status:      make(chan []*eventdef.BulkSaveStatus),
		ProcessChan: make(chan *inProgressMessage, 1000),
	}
	return q
}

func main() {
	flag.Parse()

	// Only try and parse the conf file if it exists
	if _, err := os.Stat(*confFile); err == nil {
		conf, err := globalconf.NewWithOptions(&globalconf.Options{Filename: *confFile})
		if err != nil {
			log.Fatal(4, err.Error())
		}
		conf.ParseAll()
	}

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Printf("eventtank (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	if *group == "" {
		log.Fatal(4, "--group is required")
	}

	if *topicStr == "" {
		log.Fatal(4, "--topic is required")
	}

	if *brokerStr == "" {
		log.Fatal(4, "--brokers required")
	}
	waitTime, err := time.ParseDuration(*consumerMaxWaitTime)
	if err != nil {
		log.Fatal(4, "kafka-mdm invalid config, could not parse consumer-max-wait-time: %s", err)
	}
	processingTime, err := time.ParseDuration(*consumerMaxProcessingTime)
	if err != nil {
		log.Fatal(4, "kafka-mdm invalid config, could not parse consumer-max-processing-time: %s", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, err.Error())
	}
	metrics, err := helper.New(true, *statsdAddr, *statsdType, "eventtank", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(4, err.Error())
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	initMetrics(metrics)

	writeQueue = NewInProgressMessageQueue()
	go writeQueue.Loop()
	go writeQueue.ProcessInProgress()

	err = eventdef.InitElasticsearch(*esAddr, "", "", writeQueue.status, *esBatchSize)
	if err != nil {
		log.Fatal(4, err.Error())
	}

	brokers := strings.Split(*brokerStr, ",")
	topics := strings.Split(*topicStr, ",")

	config := cluster.NewConfig()
	// see https://github.com/raintank/metrictank/issues/236
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.ClientID = strings.Replace(hostname, ".", "_", -1) + "-eventtank"
	config.Group.Return.Notifications = true
	config.ChannelBufferSize = *channelBufferSize
	config.Consumer.Fetch.Min = int32(*consumerFetchMin)
	config.Consumer.Fetch.Default = int32(*consumerFetchDefault)
	config.Consumer.MaxWaitTime = waitTime
	config.Consumer.MaxProcessingTime = processingTime
	config.Config.Version = sarama.V0_10_0_0
	err = config.Validate()
	if err != nil {
		log.Fatal(4, "invalid kafka config: %s", err)
	}
	consumer, err = cluster.NewConsumer(brokers, *group, topics, config)
	if err != nil {
		log.Fatal(4, "failed to start kafka consumer: %s", err)
	}

	go kafkaNotifications()
	doneChan := make(chan struct{})
	go Consume(doneChan)

	go func() {
		log.Info("INFO starting listener for http/debug on %s", *listenAddr)
		httperr := http.ListenAndServe(*listenAddr, nil)
		if httperr != nil {
			log.Info(httperr.Error())
		}
	}()

	for {
		select {
		case <-doneChan:
			return
		case <-sigChan:
			consumer.Close()
			eventdef.StopBulkIndexer()
		}
	}
}

func initMetrics(metrics met.Backend) {
	messagesSize = metrics.NewMeter("message_size", 0)
	msgsAge = metrics.NewMeter("message_age", 0)
	eventsToEsOK = metrics.NewCount("events_to_es.ok")
	eventsToEsFail = metrics.NewCount("events_to_es.fail")
	esPutDuration = metrics.NewTimer("es_put_duration", 0)
	msgsHandleOK = metrics.NewCount("handle.ok")
	msgsHandleFail = metrics.NewCount("handle.fail")
}

func kafkaNotifications() {
	for msg := range consumer.Notifications() {
		if len(msg.Claimed) > 0 {
			for topic, partitions := range msg.Claimed {
				log.Info("kafka consumer claimed %d partitions on topic: %s", len(partitions), topic)
			}
		}
		if len(msg.Released) > 0 {
			for topic, partitions := range msg.Released {
				log.Info("kafka consumer released %d partitions on topic: %s", len(partitions), topic)
			}
		}

		if len(msg.Current) == 0 {
			log.Info("kafka consumer is no longer consuming from any partitions.")
		} else {
			log.Info("kafka Current partitions:")
			for topic, partitions := range msg.Current {
				log.Info("kafka Current partitions: %s: %v", topic, partitions)
			}
		}
	}
	log.Info("kafka notification processing stopped")
}
