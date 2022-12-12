package model

type Transaction struct {
	ID        string `json:"id"`
	UserID    string `json:"user_id"`
	Amount    int    `json:"amount"`
	Timestamp int64  `json:"timestamp"`
}
