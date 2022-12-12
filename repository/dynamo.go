package repository

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/thezmc/dynamo-dockertest-example/model"
)

type dynamoDBTransactionRepository struct {
	client    *dynamodb.Client
	tableName string
}

func NewDynamoDBTransactionRepository(opts ...func(*dynamoDBTransactionRepository)) *dynamoDBTransactionRepository {
	repo := new(dynamoDBTransactionRepository)
	for _, opt := range opts {
		opt(repo)
	}
	return repo
}

func WithDynamoDBClient(client *dynamodb.Client) func(*dynamoDBTransactionRepository) {
	return func(repo *dynamoDBTransactionRepository) {
		repo.client = client
	}
}

func WithTableName(tableName string) func(*dynamoDBTransactionRepository) {
	return func(repo *dynamoDBTransactionRepository) {
		repo.tableName = tableName
	}
}

func (repo *dynamoDBTransactionRepository) GetTransactionsByUserID(ctx context.Context, userID string) ([]*model.Transaction, error) {
	keyExpr := expression.Key("user_id").Equal(expression.Value(userID))
	expr, err := expression.NewBuilder().WithKeyCondition(keyExpr).Build()
	if err != nil {
		return nil, err
	}

	input := &dynamodb.QueryInput{
		TableName:                 &repo.tableName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}

	output, err := repo.client.Query(ctx, input)
	if err != nil {
		return nil, err
	}

	transactions := make([]*model.Transaction, len(output.Items))
	err = attributevalue.UnmarshalListOfMapsWithOptions(output.Items, &transactions, func(opts *attributevalue.DecoderOptions) {
		opts.TagKey = "json"
	})
	if err != nil {
		return nil, err
	}

	return transactions, nil
}

func (repo *dynamoDBTransactionRepository) GetTransactionByID(ctx context.Context, userID, transactionID string) (*model.Transaction, error) {
	pk := map[string]types.AttributeValue{
		"user_id": &types.AttributeValueMemberS{Value: userID},
		"id":      &types.AttributeValueMemberS{Value: transactionID},
	}

	input := &dynamodb.GetItemInput{
		TableName: &repo.tableName,
		Key:       pk,
	}

	output, err := repo.client.GetItem(ctx, input)
	if err != nil {
		return nil, err
	}

	if output.Item == nil {
		return nil, ErrTransactionNotFound
	}

	transaction := new(model.Transaction)
	err = attributevalue.UnmarshalMapWithOptions(output.Item, transaction, func(opts *attributevalue.DecoderOptions) {
		opts.TagKey = "json"
	})
	if err != nil {
		return nil, err
	}

	return transaction, nil
}

func (repo *dynamoDBTransactionRepository) AddTransaction(ctx context.Context, transaction model.Transaction) error {
	av, err := attributevalue.MarshalMapWithOptions(transaction, func(opts *attributevalue.EncoderOptions) {
		opts.TagKey = "json"
	})
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: &repo.tableName,
		Item:      av,
	}

	_, err = repo.client.PutItem(ctx, input)
	return err
}
