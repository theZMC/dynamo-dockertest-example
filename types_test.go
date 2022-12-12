package example

import (
	"testing"

	"github.com/thezmc/dynamo-dockertest-example/repository"
)

func Test_TransactionRepository_Implementations(t *testing.T) {
	var _ TransactionRepository = repository.NewDynamoDBTransactionRepository()
}
