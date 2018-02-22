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
	mutex        sync.Mutex
	windowSize   int
	epochLimit   int
	epochTimer   *time.Ticker
	conn         *lspnet.UDPConn
	clients      map[int]*clientStub
	nextClientID int
	isClosed     bool
	closeChan    chan struct{}
	readChan     chan *Message
}

type clientStub struct {
	mutex                       sync.Mutex
	connID                      int
	isClosed                    bool
	closeChan                   chan struct{}
	addr                        *lspnet.UDPAddr
	writeChan                   chan *Message
	conn                        *lspnet.UDPConn
	seqNum                      int32
	lastProcessedMessageSeqNum  int32
	receivedMessageSeqNum       int32
	pendingReceivedMessages     map[int]*Message
	pendingReceivedMessageQueue *list.List
	pendingSendMessages         *list.List
	slideWindow                 *list.List
	pendingReSendMessages       map[int]*Message
	lastAckSeqNum               int32
	unAckedMessages             map[int]bool
	windowSize                  int
	epochFiredCount             int
	epochLimit                  int
	epochTimer                  *time.Ticker
	receivedMessageChan         chan *Message
}

type messageStub struct {
	message *Message
	isAcked bool
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
		epochLimit:   params.EpochLimit,
		epochTimer:   time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		windowSize:   params.WindowSize,
		conn:         conn,
		clients:      make(map[int]*clientStub),
		closeChan:    make(chan struct{}),
		nextClientID: 0,
		readChan:     make(chan *Message, 10),
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
				s.processAckMessage(message)
			case MsgData:
				client := s.clients[message.ConnID]
				atomic.AddInt32(&client.receivedMessageSeqNum, 1)
				client.epochFiredCount = 0
				client.writeChan <- NewAck(message.ConnID, message.SeqNum)
				client.receivedMessageChan <- message
			case MsgConnect:
				client := &clientStub{
					connID:                      int(s.nextClientID),
					isClosed:                    false,
					closeChan:                   make(chan struct{}),
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
				client.writeChan <- NewAck(client.connID, int(client.seqNum))
				s.nextClientID++
				go s.handleWrite(client)
				go client.processReceivedMessageLoop(s)
				go client.processEpochEvents()
			}
		}
	}
}

func (c *clientStub) processReceivedMessageLoop(s *server) {
	for {
		select {
		case message := <-c.receivedMessageChan:
			c.processReceivedMessage(message, s)
		}
	}

}

func (c *clientStub) processReceivedMessage(message *Message, s *server) {

	if _, ok := c.pendingReceivedMessages[message.SeqNum]; !ok && message.SeqNum >= int(c.lastProcessedMessageSeqNum) {
		c.pendingReceivedMessages[message.SeqNum] = message
		for i := int(c.lastProcessedMessageSeqNum); ; i++ {
			if _, ok := c.pendingReceivedMessages[i]; !ok {
				break
			}
			c.pendingReceivedMessageQueue.PushBack(c.pendingReceivedMessages[i])
			delete(c.pendingReceivedMessages, i)
			atomic.AddInt32(&c.lastProcessedMessageSeqNum, 1)
		}
	}

	for e := c.pendingReceivedMessageQueue.Front(); e != nil; e = c.pendingReceivedMessageQueue.Front() {
		message := e.Value.(*Message)
		if !c.isClosed {
			select {
			case s.readChan <- message:
				c.pendingReceivedMessageQueue.Remove(e)
			}
		} else {
			select {
			case s.readChan <- message:
				c.pendingReceivedMessageQueue.Remove(e)
			}
		}
	}
}

func (s *server) handleWrite(client *clientStub) {
	for {
		select {
		case message, ok := <-client.writeChan:
			if !ok {
				return
			}
			bytes, err := MarshalMessage(message)
			if err != nil {
				return
			}
			if message.Type == MsgData {
				client.pendingReSendMessages[message.SeqNum] = message
			}
			if !s.isClosed && !client.isClosed {
				_, err = client.conn.WriteToUDP(bytes, client.addr)
				if err != nil {
					return
				}
			}
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
	c := s.clients[connID]
	if c.isClosed {
		return nil
	}
	atomic.AddInt32(&c.seqNum, 1)
	message := NewData(connID, int(c.seqNum), len(payload), payload)
	if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize >= message.SeqNum {
		c.slideWindow.PushBack(message)
		c.unAckedMessages[message.SeqNum] = true
		c.writeChan <- message
		return nil
	}
	c.pendingSendMessages.PushBack(message)
	return nil
}

func (s *server) CloseConn(connID int) error {
	s.clients[connID].closeChan <- struct{}{}
	s.clients[connID].isClosed = true
	return nil
}

func (s *server) Close() error {
	s.mutex.Lock()
	for connID, client := range s.clients {
		client.closeChan <- struct{}{}
		client.isClosed = true
		delete(s.clients, connID)
	}
	s.mutex.Unlock()
	s.conn.Close()
	s.closeChan <- struct{}{}
	s.isClosed = true
	return nil
}

func (c *clientStub) resendAckMessages() {
	if atomic.LoadInt32(&c.receivedMessageSeqNum) == 0 && !c.isClosed {
		c.writeChan <- NewAck(c.connID, 0)
	} else {
		i := c.lastProcessedMessageSeqNum - 1
		for j := c.windowSize; j > 0 && i > 0; j-- {
			c.writeChan <- NewAck(c.connID, int(i))
			i--
		}
	}
}

func (c *clientStub) processEpochEvents() {
	for {
		select {
		case <-c.epochTimer.C:
			if c.isClosed {
				return
			}
			c.epochFiredCount++
			if c.epochFiredCount > c.epochLimit {
				return
			}
			c.processPendingReSendMessages()
			c.resendAckMessages()
		case <-c.closeChan:
			return
		}
	}
}

func (s *server) processAckMessage(message *Message) {
	if message.SeqNum == 0 {
		return
	}
	c := s.clients[message.ConnID]
	if message, ok := c.pendingReSendMessages[message.SeqNum]; ok {
		delete(c.pendingReSendMessages, message.SeqNum)
		delete(c.unAckedMessages, message.SeqNum)
		for e := c.slideWindow.Front(); e != nil; e = c.slideWindow.Front() {
			msg := e.Value.(*Message)
			if _, ok := c.unAckedMessages[msg.SeqNum]; ok {
				break
			} else {
				atomic.AddInt32(&c.lastAckSeqNum, 1)
				c.slideWindow.Remove(e)
			}
			c.processPendingSendMessages()
		}
	}
}

func (c *clientStub) processPendingSendMessages() {
	var next *list.Element
	for e := c.pendingSendMessages.Front(); e != nil; e = next {
		// 如果的消息超过了滑动窗口的上线，则暂存消息，等待后续处理
		next = e.Next()
		message := e.Value.(*Message)
		if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize < message.SeqNum {
			break
		}
		c.slideWindow.PushBack(message)
		c.unAckedMessages[message.SeqNum] = true
		c.pendingSendMessages.Remove(e)
		c.writeChan <- message
	}
}

func (c *clientStub) processPendingReSendMessages() {
	for _, message := range c.pendingReSendMessages {
		if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize < message.SeqNum {
			continue
		}
		if _, ok := c.unAckedMessages[message.SeqNum]; ok {
			c.writeChan <- message
		}
	}
}
