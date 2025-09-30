package validate

import (
	"errors"
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"time"

	"demo/orders/internal/model"
)

var (
	reTrack   = regexp.MustCompile(`^[A-Z0-9]{6,32}$`)
	rePhone   = regexp.MustCompile(`^\+?[0-9\s\-$begin:math:text$$end:math:text$]{5,}$`)
	reCurr    = regexp.MustCompile(`^[A-Z]{3}$`)
	reCust    = regexp.MustCompile(`^[A-Za-z0-9_\-\.]{1,64}$`)
	reOrderID = regexp.MustCompile(`^[A-Za-z0-9_\-\.]{6,64}$`)
)

type multiErr []error

func (m multiErr) Error() string {
	var b strings.Builder
	for i, e := range m {
		if i > 0 {
			b.WriteString("; ")
		}
		b.WriteString(e.Error())
	}
	return b.String()
}
func (m multiErr) OrNil() error {
	if len(m) == 0 {
		return nil
	}
	return m
}

func ValidateOrder(o model.Order) error {
	var errs multiErr

	// Базовые поля
	if !reOrderID.MatchString(o.OrderUID) {
		errs = append(errs, fmt.Errorf("order_uid: invalid or empty"))
	}
	if !reTrack.MatchString(strings.ToUpper(o.TrackNumber)) {
		errs = append(errs, fmt.Errorf("track_number: must be 6-32 uppercase letters/digits"))
	}
	if strings.TrimSpace(o.Entry) == "" {
		errs = append(errs, fmt.Errorf("entry: required"))
	}
	if o.CustomerID == "" || !reCust.MatchString(o.CustomerID) {
		errs = append(errs, fmt.Errorf("customer_id: 1..64 [A-Za-z0-9_.-]"))
	}
	if o.DateCreated.After(time.Now().Add(5 * time.Minute)) {
		errs = append(errs, fmt.Errorf("date_created: must not be in the future"))
	}

	// Delivery
	if o.Delivery.Email != "" {
		if _, err := mail.ParseAddress(o.Delivery.Email); err != nil {
			errs = append(errs, fmt.Errorf("delivery.email: invalid"))
		}
	}
	if o.Delivery.Phone != "" && !rePhone.MatchString(o.Delivery.Phone) {
		errs = append(errs, fmt.Errorf("delivery.phone: invalid"))
	}

	// Payment
	if !reCurr.MatchString(strings.ToUpper(o.Payment.Currency)) {
		errs = append(errs, fmt.Errorf("payment.currency: must be 3-letter ISO code"))
	}
	if o.Payment.Amount < 0 {
		errs = append(errs, fmt.Errorf("payment.amount: must be >= 0"))
	}
	if o.Payment.DeliveryCost < 0 || o.Payment.GoodsTotal < 0 || o.Payment.CustomFee < 0 {
		errs = append(errs, fmt.Errorf("payment costs: must be >= 0"))
	}
	payTime := time.Unix(o.Payment.PaymentDT, 0)
	if payTime.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) || payTime.After(time.Now().Add(5*time.Minute)) {
		errs = append(errs, fmt.Errorf("payment_dt: out of sane range"))
	}

	// Items
	if len(o.Items) == 0 {
		errs = append(errs, errors.New("items: must contain at least 1 item"))
	} else {
		for i, it := range o.Items {
			if it.ChrtID <= 0 {
				errs = append(errs, fmt.Errorf("items[%d].chrt_id: must be > 0", i))
			}
			if strings.TrimSpace(it.Name) == "" {
				errs = append(errs, fmt.Errorf("items[%d].name: required", i))
			}
			if it.Price < 0 || it.TotalPrice < 0 {
				errs = append(errs, fmt.Errorf("items[%d].price/total_price: must be >= 0", i))
			}
			if it.Sale < 0 || it.Sale > 100 {
				errs = append(errs, fmt.Errorf("items[%d].sale: 0..100", i))
			}
			if strings.TrimSpace(it.Size) == "" {
				errs = append(errs, fmt.Errorf("items[%d].size: required", i))
			}
			if it.NmID <= 0 {
				errs = append(errs, fmt.Errorf("items[%d].nm_id: must be > 0", i))
			}
			if strings.TrimSpace(it.TrackNumber) == "" {
				errs = append(errs, fmt.Errorf("items[%d].track_number: required", i))
			}
		}
	}

	return errs.OrNil()
}
