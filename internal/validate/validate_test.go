// internal/validate/validate_test.go
package validate_test

import (
	"testing"
	"time"

	"demo/orders/internal/model"
	"demo/orders/internal/validate"

	"github.com/stretchr/testify/require"
)

func TestValidateOrder_Valid(t *testing.T) {
	o := model.Order{
		OrderUID:    "order01",
		TrackNumber: "ABC123",
		Entry:       "WBIL",
		CustomerID:  "cust1",
		DateCreated: time.Now(),
		Delivery: model.Delivery{
			Email: "",
			Phone: "",
		},
		Payment: model.Payment{
			Currency:     "USD",
			Amount:       1000,
			DeliveryCost: 0,
			GoodsTotal:   1000,
			CustomFee:    0,
			PaymentDT:    time.Now().Unix(),
		},
		Items: []model.Item{
			{
				ChrtID:      1,
				TrackNumber: "WB123456",
				Price:       100,
				RID:         "rid-1",
				Name:        "Product",
				Sale:        0,
				Size:        "M",
				TotalPrice:  100,
				NmID:        10,
				Brand:       "B",
				Status:      200,
			},
		},
	}

	err := validate.ValidateOrder(o)
	require.NoError(t, err)
}

func TestValidateOrder_InvalidEmpty(t *testing.T) {
	o := model.Order{} // явно некорректный заказ
	err := validate.ValidateOrder(o)
	require.Error(t, err)
}
