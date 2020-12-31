package message

type Message struct {
	availableAt int
	body        interface{}
}

func (msg Message) GetBody() interface{} {
	return msg.body
}

func (msg Message) GetAvailableAt() int {
	return msg.availableAt
}

func NewMessage(body interface{}, availableAt int) Message {
	return Message{
		availableAt: availableAt,
		body:        body,
	}
}
