package message

type Message struct {
	AvailableAt int
	Body        interface{}
}

func (msg Message) GetBody() interface{} {
	return msg.Body
}

func (msg Message) GetAvailableAt() int {
	return msg.AvailableAt
}

func NewMessage(body interface{}, availableAt int) Message {
	return Message{
		AvailableAt: availableAt,
		Body:        body,
	}
}
