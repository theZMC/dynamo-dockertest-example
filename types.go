package example

import (
	"context"

	"github.com/thezmc/dynamo-dockertest-example/model"
)

type TransactionRepository interface {
	GetTransactionsByUserID(ctx context.Context, userID string) ([]*model.Transaction, error)
	GetTransactionByID(ctx context.Context, userID, transactionID string) (*model.Transaction, error)
	AddTransaction(ctx context.Context, transaction *model.Transaction) error
}
