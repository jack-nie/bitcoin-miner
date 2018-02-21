package lsp

import (
	"encoding/json"
	"errors"
)

var (
	// ErrConnClosed describe connection closed.
	ErrConnClosed = errors.New("connection closed")
	// ErrCannotEstablishConnection describe can not establish a connection between client and server.
	ErrCannotEstablishConnection = errors.New("can not establish connection")
)

// MaxMessageSize describe the max size of message
const MaxMessageSize = 1000

// MarshalMessage serielize the message
func MarshalMessage(msg *Message) ([]byte, error) {
	marshaledMsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return marshaledMsg, nil

}

// UnMarshalMessage deserielize the message
func UnMarshalMessage(buffer []byte) *Message {
	var msg Message
	json.Unmarshal(buffer, &msg)
	return &msg
}
