// Contains the implementation of a LSP server.

package lsp

import (
	"container/list"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cmu440/lspnet"
)

type server struct {
	mutex              sync.Mutex
	epochLimit         int
	epochTimer         *time.Ticker
	windowSize         int
	conn               *lspnet.UDPConn
	clients            map[int]*clientStub
	nextClientID       int
	receiveMessageChan chan *Message
	isClosed           bool
	closeChan          chan struct{}
	readChan           chan *Message
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
		epochLimit:         params.EpochLimit,
		epochTimer:         time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		windowSize:         params.WindowSize,
		conn:               conn,
		clients:            make(map[int]*clientStub),
		closeChan:          make(chan struct{}),
		nextClientID:       0,
		receiveMessageChan: make(chan *Message),
		readChan:           make(chan *Message, 10),
	}
	go server.handleConn(conn)
	go server.processEpochEvents()
	return server, nil
}

func (s *server) handleConn(conn *lspnet.UDPConn) {
	for {
		select {
		case <-s.closeChan:
			fmt.Println("close===================")
		default:

			buffer := make([]byte, MaxMessageSize)
			fmt.Println("=========++++++++++++++++++++++++++")
			n, addr, err := conn.ReadFromUDP(buffer)
			fmt.Println("heere in the loop ", err)
			if err != nil {
				return
			}
			buffer = buffer[:n]
			message := UnMarshalMessage(buffer)
			switch message.Type {
			case MsgAck:
				s.processAckMessage(message)
			case MsgData:
				s.processReceivedMessage(message)
				s.clients[message.ConnID].writeChan <- NewAck(message.ConnID, message.SeqNum)
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
					lastProcessedMessageSeqNum:  1,
					slideWindow:                 list.New(),
					lastAckSeqNum:               0,
					windowSize:                  s.windowSize,
					unAckedMessages:             make(map[int]bool),
				}
				s.mutex.Lock()
				s.clients[s.nextClientID] = client
				s.mutex.Unlock()
				client.writeChan <- NewAck(client.connID, int(client.seqNum))
				s.nextClientID++
				fmt.Println(client.addr, client.connID, len(s.clients))
				go s.handleWrite(client)
				go s.processReceivedMessageLoop(client)
			}
		}
	}
}

func (s *server) processReceivedMessageLoop(client *clientStub) {
	for {
		if client.pendingReceivedMessageQueue.Len() != 0 {
			e := client.pendingReceivedMessageQueue.Front()
			message := e.Value.(*Message)
			fmt.Println("message", message.String())
			if !client.isClosed {
				select {
				case s.readChan <- message:
					client.pendingReceivedMessageQueue.Remove(e)
				}
			} else {
				select {
				case s.readChan <- message:
					client.pendingReceivedMessageQueue.Remove(e)
				}
			}
		}
	}
}

func (s *server) processReceivedMessage(message *Message) {
	client := s.clients[message.ConnID]
	atomic.AddInt32(&client.receivedMessageSeqNum, 1)
	if _, ok := client.pendingReceivedMessages[message.SeqNum]; !ok && message.SeqNum >= int(client.receivedMessageSeqNum) {
		client.pendingReceivedMessages[message.SeqNum] = message
		for i := int(client.lastProcessedMessageSeqNum); ; i++ {
			if _, ok := client.pendingReceivedMessages[i]; !ok {
				break
			}
			client.pendingReceivedMessageQueue.PushBack(client.pendingReceivedMessages[i])
			delete(client.pendingReceivedMessages, i)
			atomic.AddInt32(&client.lastProcessedMessageSeqNum, 1)
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
				client.mutex.Lock()
				client.pendingReSendMessages[message.SeqNum] = message
				client.mutex.Unlock()
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
	c.mutex.Lock()
	c.pendingSendMessages.PushBack(message)
	c.mutex.Unlock()
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

func (s *server) resendAckMessages() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, client := range s.clients {
		if atomic.LoadInt32(&client.lastProcessedMessageSeqNum) == 0 && !client.isClosed {
			client.writeChan <- NewAck(client.connID, 0)
		}
	}
}

func (s *server) processEpochEvents() {
	for {
		select {
		case <-s.epochTimer.C:
			if s.isClosed && len(s.clients) == 0 {
				return
			}
			s.processPendingReSendMessages()
			s.resendAckMessages()
		case <-s.closeChan:
			return
		}
	}
}

func (s *server) processAckMessage(message *Message) {
	if message.SeqNum == 0 {
		return
	}
	c := s.clients[message.ConnID]
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
			fmt.Println("========ack:", message.String())
		}
	}
}

func (c *clientStub) processPendingSendMessages() {
	var next *list.Element
	for e := c.pendingSendMessages.Front(); e != nil; e = next {
		// 如果的消息超过了滑动窗口的上线，则暂存消息，等待后续处理
		next = e.Next()
		message := e.Value.(*Message)
		if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize <= message.SeqNum {
			break
		}
		c.slideWindow.PushBack(message)
		c.unAckedMessages[message.SeqNum] = true
		c.pendingSendMessages.Remove(e)
		c.writeChan <- message
	}
}

func (s *server) processPendingReSendMessages() {
	for _, c := range s.clients {
		for _, message := range c.pendingReSendMessages {
			if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize >= message.SeqNum {
				if _, ok := c.unAckedMessages[message.SeqNum]; ok {
					c.writeChan <- message
				}
			}

		}
	}
}
