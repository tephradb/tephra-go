package tephra_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	tephra "github.com/tephradb/tephra-go"
)

// Example shows the common flow: connect, append an event, then drain a read.
func Example() {
	ctx := context.Background()

	client, err := tephra.Dial(ctx, "127.0.0.1:9000")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	event, err := tephra.NewEvent("Enrolled", []string{"course:c1", "student:s1"}, []byte(`{}`))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := client.Append(ctx, []tephra.Event{event}, nil); err != nil {
		log.Fatal(err)
	}

	events, watermark, err := client.ReadAll(ctx, tephra.QueryAll(), tephra.Zero, nil)
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range events {
		fmt.Printf("%d: %s\n", e.Position, e.Type())
	}
	fmt.Printf("watermark: %d\n", watermark)
}

// ExampleClient_Append_condition guards an append with a dynamic consistency boundary: the append
// is rejected if any event matching the query already exists (a uniqueness guard).
func ExampleClient_Append_condition() {
	ctx := context.Background()
	client, err := tephra.Dial(ctx, "127.0.0.1:9000")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	item, err := tephra.WithTags("email:a@example.com")
	if err != nil {
		log.Fatal(err)
	}
	event, err := tephra.NewEvent("Registered", []string{"email:a@example.com"}, []byte(`{}`))
	if err != nil {
		log.Fatal(err)
	}

	// Fail if any event with this email already landed anywhere in the log.
	cond := tephra.NewAppendCondition(tephra.QueryItems(item))
	_, err = client.Append(ctx, []tephra.Event{event}, &cond)

	var serverErr *tephra.ServerError
	if errors.As(err, &serverErr) && serverErr.Code == tephra.ErrCodeConflict {
		fmt.Println("email already registered")
	}
}

// ExampleClient_Subscribe tails matching events live, printing each and noting when the stream
// reaches the live edge.
func ExampleClient_Subscribe() {
	ctx := context.Background()
	client, err := tephra.Dial(ctx, "127.0.0.1:9000")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	sub, err := client.Subscribe(ctx, tephra.QueryAll(), tephra.Zero)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	for sub.Next() {
		item := sub.Item()
		if item.IsCaughtUp() {
			fmt.Println("caught up, tailing live")
			continue
		}
		fmt.Printf("%d: %s\n", item.Event.Position, item.Event.Type())
	}
	if err := sub.Err(); err != nil {
		log.Fatal(err)
	}
}
