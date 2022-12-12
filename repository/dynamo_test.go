package repository

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ory/dockertest"
	"github.com/thezmc/dynamo-dockertest-example/model"
)

var testTransactions = []*model.Transaction{
	{
		ID:        "21e4e1bc-b2f8-4a47-b092-3e0c452462e0",
		UserID:    "5a0aeb2d-36c6-4400-a7e8-60f78b8e1198",
		Amount:    200,
		Timestamp: time.Now().Unix(),
	},
	{
		ID:        "a4c8c909-3925-4110-898e-176c7eb4f9a3",
		UserID:    "5a0aeb2d-36c6-4400-a7e8-60f78b8e1198",
		Amount:    100,
		Timestamp: time.Now().Unix(),
	},
	{
		ID:        "01cd3dbc-0191-49d9-80b6-e91ab46e8478",
		UserID:    "07cea472-6a29-4664-b2ce-856ea8eafd02",
		Amount:    300,
		Timestamp: time.Now().Unix(),
	},
}

func NewDynamoIntegrationTestRepository(t *testing.T) *dynamoDBTransactionRepository {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("could not connect to docker: %v", err)
	}
	resource, err := pool.Run("public.ecr.aws/aws-dynamodb-local/aws-dynamodb-local", "1.19.0", []string{})
	if err != nil {
		t.Fatalf("could not start resource: %v", err)
	}
	t.Cleanup(func() {
		if err := pool.Purge(resource); err != nil {
			t.Fatalf("could not purge resource: %v", err)
		}
	})

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           "http://localhost:" + resource.GetPort("8000/tcp"),
			SigningRegion: "us-east-1",
		}, nil
	})
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithEndpointResolverWithOptions(resolver))
	if err != nil {
		t.Fatalf("could not load config: %v", err)
	}
	client := dynamodb.NewFromConfig(cfg)

	pool.MaxWait = 60 * time.Second
	if err := pool.Retry(func() error {
		_, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
		return err
	}); err != nil {
		t.Fatalf("could not connect to dynamo container: %v", err)
	}

	_, err = client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("transactions"),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("user_id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("user_id"),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	if err != nil {
		t.Fatalf("could not create table: %v", err)
	}

	repo := NewDynamoDBTransactionRepository(WithDynamoDBClient(client), WithTableName("transactions"))
	return repo
}

func Test_dynamoDBTransactionRepository_Integration(t *testing.T) {
	repo := NewDynamoIntegrationTestRepository(t)

	for _, transaction := range testTransactions {
		if err := repo.AddTransaction(context.Background(), transaction); err != nil {
			t.Fatalf("could not add transaction: %v", err)
		}
	}

	t.Run("GetTransactionsByUserID  Multiple Transactions From User ID", func(t *testing.T) {
		transactions, err := repo.GetTransactionsByUserID(context.Background(), testTransactions[0].UserID)
		if err != nil {
			t.Fatalf("could not get transactions: %v", err)
		}
		if !reflect.DeepEqual(transactions, testTransactions[:2]) {
			t.Fatalf("expected %v, got %v", testTransactions[:2], transactions)
		}
	})

	t.Run("GetTransactionsByUserID  One Transaction From User ID", func(t *testing.T) {
		transactions, err := repo.GetTransactionsByUserID(context.Background(), testTransactions[2].UserID)
		if err != nil {
			t.Fatalf("could not get transactions: %v", err)
		}
		if !reflect.DeepEqual(transactions, testTransactions[2:]) {
			t.Fatalf("expected %v, got %v", testTransactions[2:], transactions)
		}
	})

	t.Run("GetTransactionsByUserID  No Transactions From User ID", func(t *testing.T) {
		transactions, err := repo.GetTransactionsByUserID(context.Background(), "invalid")
		if err != nil {
			t.Fatalf("could not get transactions: %v", err)
		}
		if len(transactions) != 0 {
			t.Fatalf("expected 0 transactions, got %v", transactions)
		}
	})

	t.Run("GetTransactionByID  Valid Transaction ID", func(t *testing.T) {
		transaction, err := repo.GetTransactionByID(context.Background(), testTransactions[0].UserID, testTransactions[0].ID)
		if err != nil {
			t.Fatalf("could not get transaction: %v", err)
		}
		if !reflect.DeepEqual(transaction, testTransactions[0]) {
			t.Fatalf("expected %v, got %v", testTransactions[0], transaction)
		}
	})

	t.Run("GetTransactionByID  Invalid Transaction ID", func(t *testing.T) {
		transaction, err := repo.GetTransactionByID(context.Background(), testTransactions[0].UserID, "invalid")
		if !errors.Is(err, ErrTransactionNotFound) {
			t.Fatalf("expected ErrTransactionNotFound, got %v", err)
		}
		if transaction != nil {
			t.Fatalf("expected nil, got %v", transaction)
		}
	})
}
