package kafka

type Message struct {
	MessageKey    any               `json:"messageKey" bson:"messageKey"`                   //DO NOT EDIT
	MessageValue  any               `json:"messageValue" bson:"messageValue"`               //DO NOT EDIT
	MessageHeader map[string]string `json:"messageHeaders" bson:"messageHeaders,omitempty"` //DO NOT EDIT
}
