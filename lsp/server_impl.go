// Contains the implementation of a LSP server.

package lsp

import (
	"container/list"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cmu440/lspnet"
)

type server struct {
	mutex             sync.Mutex
	windowSize        int
	epochLimit        int
	epochTimer        *time.Ticker
	conn              *lspnet.UDPConn
	clients           map[int]*client
	nextClientID      int
	isClosed          bool
	closeChan         chan int
	completeCloseChan chan int
	readChan          chan *Message
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	udpAddr, err := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	server := &server{
		epochLimit:        params.EpochLimit,
		epochTimer:        time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		windowSize:        params.WindowSize,
		conn:              conn,
		clients:           make(map[int]*client),
		closeChan:         make(chan int),
		completeCloseChan: make(chan int),
		nextClientID:      0,
		readChan:          make(chan *Message, 10),
	}

	go server.handleConn(conn)
	return server, nil
}

func (s *server) handleConn(conn *lspnet.UDPConn) {
	for {
		select {
		case <-s.closeChan:

		default:
			buffer := make([]byte, MaxMessageSize)
			n, addr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			buffer = buffer[:n]
			message := UnMarshalMessage(buffer)
			switch message.Type {
			case MsgAck:
				client := s.clients[message.ConnID]
				client.receivedMessageChan <- message
			case MsgData:
				client := s.clients[message.ConnID]
				atomic.AddInt32(&client.receivedMessageSeqNum, 1)
				atomic.StoreInt32(&client.epochFiredCount, 0)
				sendMessageToClient(client, NewAck(message.ConnID, message.SeqNum))
				client.receivedMessageChan <- message
			case MsgConnect:
				client := &client{
					connID:                      int(s.nextClientID),
					isClosed:                    false,
					closeChan:                   make(chan int),
					writeChan:                   make(chan *Message, 10),
					addr:                        addr,
					conn:                        s.conn,
					seqNum:                      0,
					pendingReceivedMessages:     make(map[int]*Message),
					pendingSendMessages:         list.New(),
					pendingReceivedMessageQueue: list.New(),
					receivedMessageSeqNum:       0,
					pendingReSendMessages:       make(map[int]*Message),
					receivedMessageChan:         make(chan *Message),
					lastProcessedMessageSeqNum:  1,
					slideWindow:                 list.New(),
					lastAckSeqNum:               0,
					unAckedMessages:             make(map[int]bool),
					epochLimit:                  s.epochLimit,
					epochTimer:                  s.epochTimer,
					windowSize:                  s.windowSize,
					epochFiredCount:             0,
				}
				s.mutex.Lock()
				s.clients[s.nextClientID] = client
				s.mutex.Unlock()
				sendMessageToClient(client, NewAck(client.connID, int(client.seqNum)))
				s.nextClientID++
				go s.handleEvents(client)
			}
		}
	}
}

func (s *server) handleEvents(c *client) {
	for {
		select {
		case message, ok := <-c.writeChan:
			if !ok {
				return
			}
			if message.Type == MsgData {
				c.pendingSendMessages.PushBack(message)
				c.processPendingSendMessages(sendMessageToClient)
			}
		case <-c.epochTimer.C:
			if c.isClosed {
				return
			}
			atomic.AddInt32(&c.epochFiredCount, 1)
			if int(atomic.LoadInt32(&c.epochFiredCount)) > c.epochLimit {
				return
			}
			c.processPendingReSendMessages(sendMessageToClient)
			c.resendAckMessages(sendMessageToClient)
		case msg := <-c.receivedMessageChan:
			c.processReceivedMessage(msg)
			s.prepareReadMessage(c)
		case <-s.closeChan:
			return
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	select {
	case message := <-s.readChan:
		return message.ConnID, message.Payload, nil
	case <-s.closeChan:
		return -1, nil, nil
	}
}

func (s *server) Write(connID int, payload []byte) error {
	if s.isClosed {
		return ErrConnClosed
	}
	c, ok := s.clients[connID]
	if !ok || c.isClosed {
		return ErrConnClosed
	}
	atomic.AddInt32(&c.seqNum, 1)
	message := NewData(connID, int(c.seqNum), len(payload), payload)

	c.writeChan <- message

	return nil
}

func (s *server) CloseConn(connID int) error {
	client, ok := s.clients[connID]
	if !ok {
		return ErrConnClosed
	}
	client.closeChan <- 1
	return nil
}

func (s *server) Close() error {
	s.mutex.Lock()
	for _, client := range s.clients {
		client.closeChan <- 1
	}
	s.mutex.Unlock()
	s.closeChan <- 1
	<-s.completeCloseChan
	s.conn.Close()
	return nil
}

func (s *server) prepareReadMessage(c *client) {
	for e := c.pendingReceivedMessageQueue.Front(); e != nil; e = c.pendingReceivedMessageQueue.Front() {
		message := e.Value.(*Message)
		select {
		case s.readChan <- message:
			c.pendingReceivedMessageQueue.Remove(e)
		}
	}
}

func sendMessageToClient(client *client, message *Message) {
	bytes, err := MarshalMessage(message)
	if err != nil {
		return
	}
	_, err = client.conn.WriteToUDP(bytes, client.addr)
	if err != nil {
		return
	}
}
