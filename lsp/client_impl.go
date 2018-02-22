// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cmu440/lspnet"
)

type client struct {
	connID                      int
	readChan                    chan *Message
	writeChan                   chan *Message
	receivedMessageChan         chan *Message
	closeChan                   chan struct{}
	conn                        *lspnet.UDPConn
	isClosed                    bool
	epochLimit                  int
	epochMills                  int
	windowSize                  int
	seqNum                      int32
	epochFiredCount             int
	epochTimer                  *time.Ticker
	firstDataMessageReceived    bool
	receivedMessageSeqNum       int32
	lastProcessedMessageSeqNum  int32
	pendingReceivedMessages     map[int]*Message
	pendingReceivedMessageQueue *list.List
	mutex                       sync.RWMutex
	pendingSendMessages         *list.List
	pendingReSendMessages       map[int]*Message
	slideWindow                 *list.List
	lastAckSeqNum               int32
	unAckedMessages             map[int]bool
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
		conn:                        conn,
		readChan:                    make(chan *Message, 10),
		writeChan:                   make(chan *Message, 10),
		connID:                      0,
		epochLimit:                  params.EpochLimit,
		epochMills:                  params.EpochMillis,
		windowSize:                  params.WindowSize,
		epochTimer:                  time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis)),
		seqNum:                      0,
		epochFiredCount:             0,
		isClosed:                    false,
		closeChan:                   make(chan struct{}),
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
	}

	bytes, err := MarshalMessage(NewConnect())
	if err != nil {
		return nil, err
	}
	_, err = c.conn.Write(bytes)
	for {
		select {
		case <-c.epochTimer.C:
			if c.epochFiredCount >= c.epochLimit {
				return nil, ErrCannotEstablishConnection
			}
			bytes, err := MarshalMessage(NewConnect())
			if err != nil {
				return nil, err
			}
			_, err = c.conn.Write(bytes)
			if err != nil {
				c.epochFiredCount++
			}
		default:
			var ack = make([]byte, MaxMessageSize)
			var n int
			n, _, err := c.conn.ReadFromUDP(ack)
			if err != nil {
				continue
			}
			c.epochFiredCount = 0
			ackMessage := UnMarshalMessage(ack[:n])
			if ackMessage.Type == MsgAck && ackMessage.SeqNum == 0 {
				c.connID = ackMessage.ConnID
				go c.handleRead()
				go c.handleEvents()
				//go c.processReceivedMessageLoop()
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
		return msg.Payload, nil
	}
}

func (c *client) sendAck(msg *Message) {
	if msg.Type == MsgData {
		ack := NewAck(msg.ConnID, msg.SeqNum)
		c.writeChan <- ack
	}
}

func (c *client) Write(payload []byte) error {
	if c.isClosed == true {
		return ErrConnClosed
	}
	atomic.AddInt32(&c.seqNum, 1)
	message := NewData(c.connID, int(c.seqNum), len(payload), payload)
	// 如果的消息超过了滑动窗口的上线，则暂存消息，等待后续处理
	if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize >= message.SeqNum {
		c.slideWindow.PushBack(message)
		c.unAckedMessages[message.SeqNum] = false
		c.writeChan <- message
		return nil
	}

	c.mutex.Lock()
	c.pendingSendMessages.PushBack(message)
	c.mutex.Unlock()
	return nil
}

func (c *client) Close() error {
	c.conn.Close()
	c.isClosed = true
	c.closeChan <- struct{}{}
	return nil
}

func (c *client) handleRead() {
	for {
		payload := make([]byte, MaxMessageSize)
		if c.isClosed {
			return
		}
		n, err := c.conn.Read(payload)
		if err != nil {
			return
		}
		message := UnMarshalMessage(payload[:n])
		c.epochFiredCount = 0
		switch message.Type {
		case MsgData:
			c.sendAck(message)
			// 若收到第一个data message，则epochTimer触发时，不用再发送ACK
			c.epochFiredCount = 0
			atomic.AddInt32(&c.receivedMessageSeqNum, 1)
			c.receivedMessageChan <- message
		case MsgAck:
			c.processAckMessage(message)
		}
	}
}

func (c *client) handleEvents() {
	for {
		select {
		case message := <-c.writeChan:
			if !c.isClosed {
				if message.Type == MsgData {
					c.pendingReSendMessages[message.SeqNum] = message
				}
				bytes, err := MarshalMessage(message)
				if err != nil {
					continue
				}
				_, err = c.conn.Write(bytes)
				if err != nil {
					continue
				}
			}
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
		case msg := <-c.receivedMessageChan:
			c.processReceivedMessage(msg)
		case <-c.closeChan:
			return
		}
	}
}

func (c *client) processReceivedMessageLoop() {
	for {
		select {
		case message := <-c.receivedMessageChan:
			c.processReceivedMessage(message)
		}
	}

}

func (c *client) processReceivedMessage(message *Message) {
	// 重置epochFiredCount，接受到的消息数目+1
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
			case c.readChan <- message:
				c.pendingReceivedMessageQueue.Remove(e)
			}
		} else {
			select {
			case c.readChan <- message:
				c.pendingReceivedMessageQueue.Remove(e)
			}
		}
	}
}

func (c *client) processAckMessage(message *Message) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
			c.processPendingSendMessages()
		}
	}
}

func (c *client) processPendingSendMessages() {
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

func (c *client) processPendingReSendMessages() {
	for _, message := range c.pendingReSendMessages {
		if int(atomic.LoadInt32(&c.lastAckSeqNum))+c.windowSize >= message.SeqNum {
			if _, ok := c.unAckedMessages[message.SeqNum]; ok {
				c.writeChan <- message
			}
		}
	}
}

func (c *client) resendAckMessages() {
	if atomic.LoadInt32(&c.lastProcessedMessageSeqNum) != 0 {
		i := c.receivedMessageSeqNum - 1
		for j := c.windowSize; j > 0 && i > 0; j-- {
			c.writeChan <- NewAck(c.connID, int(i))
			i--
		}
	} else {
		c.writeChan <- NewAck(c.connID, 0)
	}
}
