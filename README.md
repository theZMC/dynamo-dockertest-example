# Better DynamoDB Tests in Go with dockertest
## This is a companion repo for my blog post on [Better DynamoDB tests in Go with dockertest](https://zmc.dev/posts/better-dynamodb-testing-with-dockertest). For convenience, the full post content is included below.
## What is `dockertest`?
`dockertest` is a Go library that enables programmatic control of Docker containers. The only requirement is that you have Docker
(or a compatible container runtime) installed on your machine with a docker socket available to you. Having the ability to
spin up docker containers at a whim straight from your test code is invaluable for making sure application code behaves as expected
against real dependencies.

## How does that help with DynamoDB?
AWS actually publishes a container image specifically for local DynamoDB development. I wouldn't use it to test
performance, since for that you would probably need to use DynamoDB proper. However, for testing your access patterns
against a real (enough) DynamoDB instance, it's perfect. You can use `dockertest` to spin up the container locally and run your
integration tests against that instead of a mock and you'll have a lot more confidence that your code will work as expected
in production.

## Our DynamoDB Use Case
Let's pretend we have an online storefront. Most of our data is stored in relational databases, but we also have a DynamoDB table
that stores transaction data. We want to be able to query this table for all transactions for a given user as well as individual
transactions by ID. We'll also want to be able to add new transactions to the table. We don't need to worry about updating or
deleting transactions for this example. We'll create an interface for these business requirements, then implement it using the
official AWS Go SDK. Finally, we'll write some tests to make sure our implementation works as expected against a real DynamoDB
instance. First things first though, we need to decide what our data model will look like so we'll need a `Transaction` type:

### The `Transaction` Type
I like to put models in their own package, so we'll create a `models` package and put our `Transaction` type in there.
```go
type Transaction struct {
  ID        string `json:"id"`
  UserID    string `json:"user_id"`
  Amount    int    `json:"amount"`
  Timestamp int64  `json:"timestamp"`
}
```
This is a pretty simple data model. We have a unique ID for the transaction, the ID of the user who made the transaction, the
dollar amount, and the timestamp. I added the `json` tags so we can easily marshal and unmarshal our transactions to and from
JSON. Now let's create an interface for manipulating these transactions:

### The `TransactionRepository` Interface
```go
type TransactionRepository interface {
  GetTransactionsByUserID(ctx context.Context, userID string) ([]*model.Transaction, error)
  GetTransactionByID(ctx context.Context, userID, transactionID string) (*model.Transaction, error)
  AddTransaction(ctx context.Context, transaction *model.Transaction) error
}
```
This interface is pretty straightforward. We have two methods for retrieving transactions, one by user ID and one by transaction
ID. We also have a method for adding new transactions to the table.

> #### ⚠️ Note
> You'll notice that `GetTransactionByID` takes a `userID` as well as a `transactionID`. This is due
> to the nature of DynamoDB's keying system. DynamoDB tables are keyed by a partition (hash) key and an optional sort (range) key. In our case,
> because we want to quickly retrieve all transactions for a given user, we'll use the user ID as the partition key and the transaction ID as the
> sort key. The unfortunate side effect of this is that we can't retrieve a transaction by its ID alone. We need to know the user ID as well. This
> isn't a huge deal, but it's something to keep in mind when designing your data model to work with DynamoDB. It also means
> that your interface leaks a bit of implementation detail, but not so much that your interface becomes useless for other
> implementations, so we can overlook it.

## Implementing the Repository
Now that we have our interface defined and we know what our data model looks like, we can implement our repository. I'm
going to use v2 of the official AWS Go SDK for this. You can go ahead and install the packages we'll need with `go get`:
```shell
go get github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue
go get github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression
go get github.com/aws/aws-sdk-go-v2/service/dynamodb
go get github.com/aws/aws-sdk-go-v2/config
```
Since we're here, let's also install `dockertest`:
```shell
go get github.com/ory/dockertest
```

### The `dynamoDBTransactionRepository` Type
Let's create a new type for our Dynamo-based implementation of the `TransactionRepository` interface:
```go
type dynamoDBTransactionRepository struct {
  client *dynamodb.Client // we're using SDK v2, so it's a *dynamodb.Client instead of a *dynamodb.DynamoDB
  tableName string
}
```
In this struct, we keep a reference to a `dynamodb.Client` and a `tableName`. I'm a huge fan of the [functional options pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis),
so I'm going to use it here to handle dependency injection. We'll create a constructor function and a few options:

### The `NewDynamoDBTransactionRepository` Function
```go
func NewDynamoDBTransactionRepository(opts ...func(*dynamoDBTransactionRepository)) *dynamoDBTransactionRepository {
  repo := new(dynamoDBTransactionRepository)
  for _, opt := range opts {
    opt(repo)
  }
  return repo
}
```
This is a pretty simple constructor. We create a new instance of our repository, then loop through our options and call each
one with our repository as an argument. This allows us to pass in any number of options to our constructor, and each option
can modify the repository however it needs to. This small example doesn't really show the power of this pattern, but it's
useful if your dependencies are more complex or could grow over time. The pattern allows you to add new options later
without changing your API.

### The `WithDynamoDBClient` Option
```go
func WithDynamoDBClient(client *dynamodb.Client) func(*dynamoDBTransactionRepository) {
  return func(repo *dynamoDBTransactionRepository) {
    repo.client = client
  }
}
```
This option allows us to pass in a pre-configured DynamoDB client. This is useful if we want to use a custom configuration
for our client, like using a different region or using a different credential provider. It will also be useful for testing
with a local DynamoDB instance for integration tests or a mock DynamoDB client for unit tests.

### The `WithTableName` Option
```go
func WithTableName(tableName string) func(*dynamoDBTransactionRepository) {
  return func(repo *dynamoDBTransactionRepository) {
    repo.tableName = tableName
  }
}
```
This option will simply set the name of the table we're using to store our transactions. Useful for testing or if we want to
use a different table for different sandbox environments.

### `GetTransactionsByUserID`
```go
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
```
I'm a huge proponent of using v2 of the AWS SDK's built-in expression builders to build our queries. They're much easier to read
and maintain than raw strings, and you're far less like to make a mistake. The `expression` package is also very flexible, so
you can build up complex queries with ease. Our case is pretty simple, so we just need to build a key expression to filter
our results by the user ID. We then build our expression and use it to build our `QueryInput` struct. We then call the
`Query` method on our DynamoDB client to execute our query and we unmarshal the results into a slice of `Transaction` structs.
We do have to give the unmarshaller a hint about which tag to use for unmarshalling since we didn't use the default `dynamodbav`
struct tag. I'm defining the functional option to achieve this inline here, but if you're using this pattern in a lot of places,
it's probably worth defining it in one place and reusing it.

### `GetTransactionByID`
```go
var ErrTransactionNotFound = errors.New("transaction not found")

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
```
This method is a bit more complicated than the `GetTransactionsByUserID` method. We need to build a composite primary key
to retrieve a transaction by its ID. We then build our `GetItemInput` struct and call the `GetItem` method on our DynamoDB
client to execute our query. We then unmarshal the result into a `Transaction` struct. One thing to note is that we need
to check if the `Item` field on our `GetItemOutput` struct is `nil`. If it is, that means the item we're trying to retrieve
doesn't exist. I like to return a custom error in this case so that I can handle it in my application code. Errors like
these are simple to define and can be useful for an API controller to know if it should return a 404 or a 500. You'll
notice I've created an `ErrTransactionNotFound` error here for demonstration purposes.

### `AddTransaction`
```go
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
```
This one's the simplest of the bunch. We just need to marshal our `Transaction` struct into a map of `AttributeValue`s and
then build our `PutItemInput` struct using that map. We then call `PutItem` on our DynamoDB client to execute our query.

## On to Testing
Now that we have our repository implemented, we can start writing some tests. This article doesn't cover unit testing, so
we'll just focus on integration tests. I'm going to assume you have some knowledge of how to write tests in Go, so I won't go
into painstaking detail about the process, but [check here if you need a refresher on testing in Go](https://go.dev/doc/tutorial/add-a-test).

### Create our Test Data
We'll need some sample transactions to test with. I'll just create a slice of `Transaction`s at the top of our test file.
```go
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
```
> #### ⚠️ Note
> Be mindful of the `ID` field when creating your test data. The `ID` field is your sort key and must be unique for each item
> in your table. Additionally, the `ID` field will be sorted in ascending order, so if you're comparing the results of a query
> to a slice of `Transaction`s, you'll need to make sure the `ID` field is sorted in ascending order in the slice you're
> comparing against.

### Create a Test Suite
This is where things get a little more interesting. In order to test our repository implementation against a real (enough) DynamoDB instance,
we'll need to:

* use [dockertest](https://github.com/ory/dockertest) to spin up a local DynamoDB instance
* verify that the DynamoDB instance is ready to accept connections
* create a table in the DynamoDB instance
* create a `TransactionRepository` instance that uses this dockerized DynamoDB instance

It'll be best to do all of this in one helper function, but I don't want to vomit code here, so we'll take it one step at a time.
#### Spin up a Local DynamoDB Instance
```go
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
```
Let's look at each chunk of code here. First, we mark our function as a test helper function by calling `t.Helper()`. This
will help us get more accurate error messages when our tests fail. Next, we skip this test if we're running in short mode.
This is a good practice to follow when writing integration tests. There are times we only want to run unit tests, and we
can do that by passing the `-short` flag to `go test`.

Next, we create a new `dockertest.Pool` instance. You can think of a `Pool` as a connection to a Docker daemon. The argument
we pass to `NewPool` is the Docker daemon's address. If we pass an empty string, `dockertest` will try to connect to the
default Docker daemon address at `/var/run/docker.sock`. Our test fails right here if we can't connect to the Docker daemon.

> #### ⚠️ Note
> If you're running your tests in a container, you'll need to mount the Docker socket into your container. You can do this
> by adding the following to your `docker run` command:
> ```shell
> -v /var/run/docker.sock:/var/run/docker.sock
> ```

Next, we create our dynamodb-local container resource. We pass the name and version of the image we want to use, as well as
any environment variables we want to set. In this case, we don't need to set any environment variables, so we just pass an
empty slice. Our test fails right here if we can't create the container resource. I've given

Immediately, we register a cleanup function to our test. This function will be called when our test finishes, whether it passes or
fails. We use this function to clean up our container resource. If we don't do this, we'll end up with a bunch of dangling
containers on our machine.

#### Create a DynamoDB Client Configured to Use Our Local DynamoDB Instance
```go
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
```
This looks a bit messy, but it's [the documented way](https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/endpoints/#overriding-endpoint-with-fallback)
to configure an alternative endpoint. The real meat and potatoes is in the `URL` field of the `aws.Endpoint` struct.
We're telling the client to connect to our local DynamoDB instance and we're asking `dockertest` which local port our
container is listening on. We then use this endpoint to create a new `dynamodb.Client` instance.

#### Verify That Our Local DynamoDB Instance Is Ready to Accept Connections
```go
  pool.MaxWait = 60 * time.Second
  if err := pool.Retry(func() error {
    _, err := client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
    return err
  }); err != nil {
    t.Fatalf("could not connect to dynamo container: %v", err)
  }
```
Here we're setting our maximum wait time to 60 seconds. This is the maximum amount of time we'll wait for our container to
be ready to accept connections. We then use `pool.Retry` to specify a function that we want to retry until it succeeds. In
this case, we're just trying to list the tables in our DynamoDB instance. We don't really care what tables are in there, we
just want to make sure we can connect to the instance using our client. If, after 60 seconds, our function hasn't succeeded,
we fail our test.

#### Create a Table in Our Local DynamoDB Instance
```go
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
```
Now that we have a client that can connect to our local DynamoDB instance, we can create a table in it. We're creating a
table called `transactions` with a composite primary key of `user_id` and `id`. We're also setting the read and write
capacity units to 1. This is the minimum amount of capacity we can set for a table and we don't actually care, but the
client will fail to write or read from the table if we don't set it.
> #### ⚠️ Note
> Here's where you're setting the partition key and sort key for your table. Make sure these match up with the partition key
> and sort key you've defined (or will define) on your actual transactions table in DynamoDB, otherwise this test will be useless.

#### Create a Repository Instance and Return it
```go
  repo := NewDynamoDBTransactionRepository(WithDynamoDBClient(client), WithTableName("transactions"))
  return repo
```
Finally, we create a new `DynamoDBTransactionRepository` instance and return it. We're passing in our `dynamodb.Client`
instance and the name of our local DynamoDB table, `transactions`.

#### Our Completed Test Helper
```go
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
```

### Writing Our Tests
Now that we have our test helper, we can write our tests. Since this is an integration test, it's going to look a little
different than the table-driven tests I usually use for unit testing. I want to test that transactions I write to the
repository are actually written to dynamo and I want to make sure I can read them back out. To that end, we're going to create
one parent test that writes all of our transactions to Dynamo using our repository and then we'll create subtests that read
(or attempt to read) that data back out.

#### Create a Test Function
```go
func Test_dynamoDBTransactionRepository_Integration(t *testing.T) {
  repo := NewDynamoIntegrationTestRepository(t)

  for _, transaction := range testTransactions {
    if err := repo.AddTransaction(context.Background(), transaction); err != nil {
      t.Fatalf("could not add transaction: %v", err)
    }
  }
```
Here, we've used our test helper to create a new `dynamoDBTransactionRepository` instance. We then loop over our test
transactions and use the repository to add them to DynamoDB. This serves two purposes. First, it ensures that our test
data is ready to go when we run our subtests. Second, it serves as an integration test for our `AddTransaction` method.

Next, let's create a subtest to read some of our transactions back out.

#### Testing `GetTransactionsByUserID`
```go
  t.Run("GetTransactionsByUserID  Multiple Transactions From User ID", func(t *testing.T) {
    transactions, err := repo.GetTransactionsByUserID(context.Background(), testTransactions[0].UserID)
    if err != nil {
      t.Fatalf("could not get transactions: %v", err)
    }
    if !reflect.DeepEqual(transactions, testTransactions[:2]) {
      t.Fatalf("expected %v, got %v", testTransactions[:2], transactions)
    }
  })
```
You may have noticed in our test data that we have two transactions with the same user ID. We can use this to test our
`GetTransactionsByUserID` method. We call the method with the user ID of the first transaction in our test data and then
assert that we get back the first **two** transactions in our test data, the ones with the same user ID.

Let's do a couple more subtests with `GetTransactionsByUserID`:
```go
  t.Run("GetTransactionsByUserID  One Transaction From User ID", func(t *testing.T) {
    transactions, err := repo.GetTransactionsByUserID(context.Background(), testTransactions[2].UserID)
    if err != nil {
      t.Fatalf("could not get transactions: %v", err)
    }
    if !reflect.DeepEqual(transactions, testTransactions[2:]) {
      t.Fatalf("expected %v, got %v", testTransactions[2:], transactions)
    }
  })
```
Here, we're testing a `UserID` that we know only has one transaction associated with it. We assert that we get back the
last transaction in our test data -- the one with that same user ID.

```go
  t.Run("GetTransactionsByUserID  No Transactions From User ID", func(t *testing.T) {
    transactions, err := repo.GetTransactionsByUserID(context.Background(), "invalid")
    if err != nil {
      t.Fatalf("could not get transactions: %v", err)
    }
    if len(transactions) != 0 {
      t.Fatalf("expected 0 transactions, got %v", transactions)
    }
  })
```
Finally, we're testing a `UserID` that we know doesn't exist in our test data. We assert that we get back an empty slice.

#### Testing `GetTransactionByID`
```go
  t.Run("GetTransactionByID  Valid Transaction ID", func(t *testing.T) {
    transaction, err := repo.GetTransactionByID(context.Background(), testTransactions[0].UserID, testTransactions[0].ID)
    if err != nil {
      t.Fatalf("could not get transaction: %v", err)
    }
    if !reflect.DeepEqual(transaction, testTransactions[0]) {
      t.Fatalf("expected %v, got %v", testTransactions[0], transaction)
    }
  })
```
In this test, we're testing our last untested method, `GetTransactionByID`. We're testing using the `UserID` and `ID` of the
first transaction in our test data. We assert that we get back the same transaction.

```go
  t.Run("GetTransactionByID  Invalid Transaction ID", func(t *testing.T) {
    transaction, err := repo.GetTransactionByID(context.Background(), testTransactions[0].UserID, "invalid")
    if !errors.Is(err, ErrTransactionNotFound) {
      t.Fatalf("expected ErrTransactionNotFound, got %v", err)
    }
    if transaction != nil {
      t.Fatalf("expected nil, got %v", transaction)
    }
  })
```
Finally, we're testing a `UserID` and `ID` that we know don't exist in our test data. We assert that we get back a
`ErrTransactionNotFound` error and a `nil` transaction.

Putting it all together, our full `Test_dynamoDBTransactionRepository_Integration` function looks like this:
```go
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
```

#### Running Our Tests
Now that we have our tests written, we can run them with `go test` from the root of the project:
```bash
go test -v -cover ./...
```
This will run all of our tests and output the results. We should see something like this:
```
=== RUN   Test_dynamoDBTransactionRepository_Integration
=== RUN   Test_dynamoDBTransactionRepository_Integration/GetTransactionsByUserID__Multiple_Transactions_From_User_ID
=== RUN   Test_dynamoDBTransactionRepository_Integration/GetTransactionsByUserID__One_Transaction_From_User_ID
=== RUN   Test_dynamoDBTransactionRepository_Integration/GetTransactionsByUserID__No_Transactions_From_User_ID
=== RUN   Test_dynamoDBTransactionRepository_Integration/GetTransactionByID__Valid_Transaction_ID
=== RUN   Test_dynamoDBTransactionRepository_Integration/GetTransactionByID__Invalid_Transaction_ID
--- PASS: Test_dynamoDBTransactionRepository_Integration (2.81s)
    --- PASS: Test_dynamoDBTransactionRepository_Integration/GetTransactionsByUserID__Multiple_Transactions_From_User_ID (0.07s)
    --- PASS: Test_dynamoDBTransactionRepository_Integration/GetTransactionsByUserID__One_Transaction_From_User_ID (0.00s)
    --- PASS: Test_dynamoDBTransactionRepository_Integration/GetTransactionsByUserID__No_Transactions_From_User_ID (0.00s)
    --- PASS: Test_dynamoDBTransactionRepository_Integration/GetTransactionByID__Valid_Transaction_ID (0.02s)
    --- PASS: Test_dynamoDBTransactionRepository_Integration/GetTransactionByID__Invalid_Transaction_ID (0.00s)
PASS
coverage: 85.7% of statements
```
You can see that our integration tests ran and passed and we're even at 85.7% coverage without any mocks or stubs! Not bad.

## Conclusion
As you can see, tools like `dockertest` as well as AWS-provided resources like `dynamodb-local` make it trivial to write
robust integration tests for your Go applications. Not only are the tests reasonably fast, they also provide you and your
team with confidence that your application performs as expected in a production-like environment. Hopefully, you can build
on this pattern to create end-to-end tests that spin up multiple services and test your application's behavior in even more
realistic scenarios.

The full source code for this article can be found on [my GitHub](https://github.com/theZMC/dynamo-dockertest-example).

## Resources
* [dockertest](https://github.com/ory/dockertest)
* [AWS Documentation for DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
* [`dynamodb-local` Container Image](https://gallery.ecr.aws/aws-dynamodb-local/aws-dynamodb-local)