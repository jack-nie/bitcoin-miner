// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cmu440/lspnet"
)

type client struct {
	mutex sync.RWMutex

	connID                      int
	readChan                    chan *Message
	writeChan                   chan *Message
	receivedMessageChan         chan *Message
	conn                        *lspnet.UDPConn
	windowSize                  int
	seqNum                      int32
	firstDataMessageReceived    bool
	receivedMessageSeqNum       int32
	lastProcessedMessageSeqNum  int32
	pendingReceivedMessages     map[int]*Message
	pendingReceivedMessageQueue *list.List
	pendingSendMessages         *list.List
	pendingReSendMessages       map[int]*Message
	slideWindow                 *list.List
	lastAckSeqNum               int32
	unAckedMessages             map[int]bool

	// epoch
	epochFiredCount int32
	epochTimer      *time.Ticker
	epochLimit      int
	epochMillis     int

	// 退出相关
	isClosed              int32
	isLost                int32
	eventsRoutineExitChan chan int
	toExitRoutineChan     chan int
	readRoutineExitChan   chan int
	closeChan             chan int
	toCloseChan           chan int

	addr *lspnet.UDPAddr
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	udpAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}
	c := &client{
		conn:      conn,
		readChan:  make(chan *Message, 10),
		writeChan: make(chan *Message, 10),
		connID:    0,

		epochLimit:                  params.EpochLimit,
		epochMillis:                 params.EpochMillis,
		windowSize:                  params.WindowSize,
		epochTimer:                  time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		seqNum:                      0,
		epochFiredCount:             0,
		firstDataMessageReceived:    true,
		receivedMessageSeqNum:       0,
		lastProcessedMessageSeqNum:  1,
		lastAckSeqNum:               0,
		slideWindow:                 list.New(),
		pendingReceivedMessages:     make(map[int]*Message),
		pendingReceivedMessageQueue: list.New(),
		pendingSendMessages:         list.New(),
		pendingReSendMessages:       make(map[int]*Message),
		unAckedMessages:             make(map[int]bool),
		receivedMessageChan:         make(chan *Message),
		isClosed:                    0,
		isLost:                      0,
		closeChan:                   make(chan int),
		toCloseChan:                 make(chan int),
		eventsRoutineExitChan:       make(chan int),
		toExitRoutineChan:           make(chan int),
		readRoutineExitChan:         make(chan int),
	}

	bytes, err := MarshalMessage(NewConnect())
	if err != nil {
		return nil, err
	}
	_, err = c.conn.Write(bytes)
	var ack = make([]byte, MaxMessageSize)
	for {
		select {
		case <-c.epochTimer.C:
			atomic.AddInt32(&c.epochFiredCount, 1)
			if int(atomic.LoadInt32(&c.epochFiredCount)) > c.epochLimit {
				return nil, ErrCannotEstablishConnection
			}
			bytes, err := MarshalMessage(NewConnect())
			if err != nil {
				return nil, err
			}
			_, err = c.conn.Write(bytes)
			if err != nil {
				continue
			}
		default:
			ackMessage, _, err := c.clientRecvMessage(ack)
			if err != nil {
				continue
			}
			atomic.StoreInt32(&c.epochFiredCount, 0)
			if ackMessage.Type == MsgAck && ackMessage.SeqNum == 0 {
				c.connID = ackMessage.ConnID
				go c.handleRead()
				go c.handleEvents()
				return c, nil
			}
		}
	}

}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	select {
	case <-c.closeChan:
		return nil, ErrConnClosed
	case msg := <-c.readChan:
		if msg.SeqNum == -1 {
			return msg.Payload, ErrConnClosed
		}
		return msg.Payload, nil
	}
}

func (c *client) sendAck(msg *Message) {
	sendMessageToServer(c, NewAck(msg.ConnID, msg.SeqNum))
}

func (c *client) Write(payload []byte) error {
	if c.isConnClosed() || c.isConnLost() {
		return ErrConnClosed
	}
	atomic.AddInt32(&c.seqNum, 1)
	message := NewData(c.connID, int(c.seqNum), len(payload), payload)
	// 如果的消息超过了滑动窗口的上线，则暂存消息，等待后续处理
	c.writeChan <- message
	return nil
}

func (c *client) isConnClosed() bool {
	if atomic.LoadInt32(&c.isClosed) == 0 {
		return false
	}
	return true
}

func (c *client) isConnLost() bool {
	if atomic.LoadInt32(&c.isLost) == 0 {
		return false
	}
	return true
}

func (c *client) Close() error {
	c.toCloseChan <- 1
	<-c.toExitRoutineChan

	c.conn.Close()
	return nil
}

func (c *client) handleRead() {
	for {
		select {
		case <-c.closeChan:
			c.toExitRoutineChan <- 1
			return
		default:
			payload := make([]byte, MaxMessageSize)
			message, _, err := c.clientRecvMessage(payload)
			if err != nil {
				continue
			}
			atomic.StoreInt32(&c.epochFiredCount, 0)
			switch message.Type {
			case MsgData:
				c.writeChan <- NewAck(message.ConnID, message.SeqNum)
				atomic.StoreInt32(&c.epochFiredCount, 0)
				// 若收到第一个data message，则epochTimer触发时，不用再发送ACK
				atomic.AddInt32(&c.receivedMessageSeqNum, 1)
				c.receivedMessageChan <- message
			case MsgAck:
				c.receivedMessageChan <- message
			}
		}
	}
}

func (c *client) handleEvents() {
	for {
		if c.pendingReceivedMessageQueue.Len() != 0 {
			c.prepareReadMessage()
		}

		if atomic.LoadInt32(&c.isLost) != 0 {
			c.closeChan <- 1
			return
		}

		select {
		case message := <-c.writeChan:
			if !c.isConnClosed() {
				switch message.Type {
				case MsgData:
					c.pendingSendMessages.PushBack(message)
					c.processPendingSendMessages(sendMessageToServer)
				case MsgAck:
					c.sendAck(message)
				}
			}
		case <-c.epochTimer.C:
			atomic.AddInt32(&c.epochFiredCount, 1)
			if int(atomic.LoadInt32(&c.epochFiredCount)) > c.epochLimit {
				atomic.StoreInt32(&c.isLost, 1)
				c.epochTimer.Stop()
				return
			}
			c.processPendingReSendMessages(sendMessageToServer)
			c.resendAckMessages(sendMessageToServer)

		case msg := <-c.receivedMessageChan:
			switch msg.Type {
			case MsgAck:
				c.processAckMessage(msg, sendMessageToServer)
			case MsgData:
				c.processReceivedMessage(msg)
			}
			if c.checkCloseComplete() {
				c.closeChan <- 1
				return
			}
			c.prepareReadMessage()

		case <-c.toCloseChan:
			if c.processCloseChan() {
				c.closeChan <- 1
				return
			}
		}
	}
}

func (c *client) processReceivedMessage(message *Message) {
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
}

func (c *client) checkCloseComplete() bool {
	if c.isConnClosed() && c.pendingSendMessages.Len() == 0 && len(c.pendingReSendMessages) == 0 && len(c.unAckedMessages) == 0 && len(c.writeChan) == 0 {
		return true
	}
	return false
}

func (c *client) processCloseChan() bool {
	atomic.StoreInt32(&c.isClosed, 1)
	c.epochTimer.Stop()
	if c.pendingSendMessages.Len() == 0 && len(c.pendingReSendMessages) == 0 && len(c.unAckedMessages) == 0 && len(c.writeChan) == 0 {
		return true
	}
	return false
}

func (c *client) prepareReadMessage() {
	for e := c.pendingReceivedMessageQueue.Front(); e != nil; e = c.pendingReceivedMessageQueue.Front() {
		message := e.Value.(*Message)

		select {
		case c.readChan <- message:
			c.pendingReceivedMessageQueue.Remove(e)
		case <-c.toCloseChan:
			if c.processCloseChan() {
				c.closeChan <- c.connID
				return
			}
		}
	}
}

func (c *client) processAckMessage(message *Message, sendMessage func(*client, *Message)) {
	if message.SeqNum == 0 {
		return
	}
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
			c.processPendingSendMessages(sendMessage)
		}
	}
}

func (c *client) processPendingSendMessages(sendMessage func(*client, *Message)) {
	var next *list.Element
	for e := c.pendingSendMessages.Front(); e != nil; e = next {
		// 如果的消息超过了滑动窗口的上线，则暂存消息，等待后续处理
		next = e.Next()
		message := e.Value.(*Message)
		if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize < message.SeqNum {
			return
		}
		c.slideWindow.PushBack(message)
		c.pendingReSendMessages[message.SeqNum] = message
		c.unAckedMessages[message.SeqNum] = true
		c.pendingSendMessages.Remove(e)
		sendMessage(c, message)
	}
}

func (c *client) processPendingReSendMessages(sendMessage func(*client, *Message)) {
	for _, message := range c.pendingReSendMessages {
		if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize >= message.SeqNum {
			if _, ok := c.unAckedMessages[message.SeqNum]; ok {
				sendMessage(c, message)
			}
		}
	}
}

func (c *client) resendAckMessages(sendMessage func(*client, *Message)) {
	if atomic.LoadInt32(&c.receivedMessageSeqNum) == 0 {
		sendMessage(c, NewAck(c.connID, 0))
	} else {
		i := c.lastProcessedMessageSeqNum - 1
		for j := c.windowSize; j > 0 && i > 0; j-- {
			sendMessage(c, NewAck(c.connID, int(i)))
			i--
		}
	}
}

func sendMessageToServer(c *client, message *Message) {
	bytes, err := MarshalMessage(message)
	if err != nil {
		return
	}
	_, err = c.conn.Write(bytes)
	if err != nil {
		return
	}
}

func (c *client) clientRecvMessage(readBytes []byte) (*Message, *lspnet.UDPAddr, error) {
	c.conn.SetReadDeadline(time.Now().Add(time.Millisecond * time.Duration(c.epochMillis)))
	readSize, rAddr, err := c.conn.ReadFromUDP(readBytes)
	if err != nil {
		return nil, nil, err
	}
	var msg Message
	err = json.Unmarshal(readBytes[:readSize], &msg)
	if err != nil {
		return nil, nil, err
	}
	return &msg, rAddr, nil
}
