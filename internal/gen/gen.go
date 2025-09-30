package gen

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/segmentio/kafka-go"

	"demo/orders/internal/model"
)

func SeedOnce() { gofakeit.Seed(time.Now().UnixNano()) }

func FakeOrder() model.Order {
	return model.Order{
		OrderUID:    gofakeit.UUID(),
		TrackNumber: strings.ToUpper(gofakeit.LetterN(4) + gofakeit.DigitN(6)),
		Entry:       "WBIL",
		Delivery: model.Delivery{
			Name:    gofakeit.Name(),
			Phone:   gofakeit.Phone(),
			Zip:     gofakeit.Zip(),
			City:    gofakeit.City(),
			Address: gofakeit.Street(),
			Region:  gofakeit.State(),
			Email:   gofakeit.Email(),
		},
		Payment: model.Payment{
			Transaction:  gofakeit.UUID(),
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       gofakeit.Number(100, 100000),
			PaymentDT:    time.Now().Unix(),
			Bank:         "alpha",
			DeliveryCost: 500,
			GoodsTotal:   200,
			CustomFee:    0,
		},
		Items: []model.Item{{
			ChrtID:      int64(gofakeit.Number(1000, 999999)),
			TrackNumber: "WB" + strings.ToUpper(gofakeit.LetterN(2)+gofakeit.DigitN(6)),
			Price:       gofakeit.Number(10, 10000),
			RID:         gofakeit.UUID(),
			Name:        gofakeit.ProductName(),
			Sale:        gofakeit.Number(0, 90),
			Size:        "M",
			TotalPrice:  gofakeit.Number(10, 10000),
			NmID:        int64(gofakeit.Number(1000, 999999)),
			Brand:       gofakeit.Company(),
			Status:      200,
		}},
		Locale:      "en",
		CustomerID:  gofakeit.Username(),
		DateCreated: time.Now().UTC(),
	}
}

func SendOrder(ctx context.Context, w *kafka.Writer, o model.Order, source string) (int, error) {
	val, _ := json.Marshal(o)
	err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(o.OrderUID),
		Value: val,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "source", Value: []byte(source)},
		},
	})
	if err != nil {
		return 0, err
	}
	log.Printf("produced key=%s src=%s", o.OrderUID, source)
	return 1, nil
}
