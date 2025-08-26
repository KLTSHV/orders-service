package store

import (
	"context"
	"errors"
	"time"

	"demo/orders/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Repo struct {
	Pool PgxIface
}

type PgxIface interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func New(pool PgxIface) *Repo { return &Repo{Pool: pool} }

func (r *Repo) UpsertOrder(ctx context.Context, o model.Order) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_uid) DO UPDATE SET
		  track_number=EXCLUDED.track_number, entry=EXCLUDED.entry, locale=EXCLUDED.locale,
		  internal_signature=EXCLUDED.internal_signature, customer_id=EXCLUDED.customer_id,
		  delivery_service=EXCLUDED.delivery_service, shardkey=EXCLUDED.shardkey,
		  sm_id=EXCLUDED.sm_id, date_created=EXCLUDED.date_created, oof_shard=EXCLUDED.oof_shard
	`, o.OrderUID, o.TrackNumber, o.Entry, o.Locale, o.InternalSignature, o.CustomerID, o.DeliveryService, o.ShardKey, o.SmID, o.DateCreated, o.OofShard)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (order_uid) DO UPDATE SET
		  name=EXCLUDED.name, phone=EXCLUDED.phone, zip=EXCLUDED.zip, city=EXCLUDED.city,
		  address=EXCLUDED.address, region=EXCLUDED.region, email=EXCLUDED.email
	`, o.OrderUID, o.Delivery.Name, o.Delivery.Phone, o.Delivery.Zip, o.Delivery.City, o.Delivery.Address, o.Delivery.Region, o.Delivery.Email)
	if err != nil {
		return err
	}

	payTime := time.Unix(o.Payment.PaymentDT, 0).UTC()
	_, err = tx.Exec(ctx, `
		INSERT INTO payments (order_uid, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (order_uid) DO UPDATE SET
		  transaction=EXCLUDED.transaction, request_id=EXCLUDED.request_id, currency=EXCLUDED.currency,
		  provider=EXCLUDED.provider, amount=EXCLUDED.amount, payment_dt=EXCLUDED.payment_dt,
		  bank=EXCLUDED.bank, delivery_cost=EXCLUDED.delivery_cost, goods_total=EXCLUDED.goods_total, custom_fee=EXCLUDED.custom_fee
	`, o.OrderUID, o.Payment.Transaction, o.Payment.RequestID, o.Payment.Currency, o.Payment.Provider, o.Payment.Amount, payTime, o.Payment.Bank, o.Payment.DeliveryCost, o.Payment.GoodsTotal, o.Payment.CustomFee)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `DELETE FROM items WHERE order_uid=$1`, o.OrderUID)
	if err != nil {
		return err
	}

	for _, it := range o.Items {
		_, err = tx.Exec(ctx, `
			INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		`, o.OrderUID, it.ChrtID, it.TrackNumber, it.Price, it.RID, it.Name, it.Sale, it.Size, it.TotalPrice, it.NmID, it.Brand, it.Status)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (r *Repo) GetOrder(ctx context.Context, orderUID string) (model.Order, bool, error) {
	var o model.Order
	row := r.Pool.QueryRow(ctx, `
		SELECT o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature, o.customer_id,
		       o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
		       d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
		       p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt, p.bank, p.delivery_cost, p.goods_total, p.custom_fee
		FROM orders o
		JOIN deliveries d ON d.order_uid = o.order_uid
		JOIN payments  p ON p.order_uid = o.order_uid
		WHERE o.order_uid=$1`, orderUID)

	var payTime time.Time
	err := row.Scan(&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature, &o.CustomerID,
		&o.DeliveryService, &o.ShardKey, &o.SmID, &o.DateCreated, &o.OofShard,
		&o.Delivery.Name, &o.Delivery.Phone, &o.Delivery.Zip, &o.Delivery.City, &o.Delivery.Address, &o.Delivery.Region, &o.Delivery.Email,
		&o.Payment.Transaction, &o.Payment.RequestID, &o.Payment.Currency, &o.Payment.Provider, &o.Payment.Amount, &payTime, &o.Payment.Bank, &o.Payment.DeliveryCost, &o.Payment.GoodsTotal, &o.Payment.CustomFee)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Order{}, false, nil
		}
		return model.Order{}, false, err
	}
	o.Payment.PaymentDT = payTime.Unix()

	rows, err := r.Pool.Query(ctx, `SELECT chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status FROM items WHERE order_uid=$1`, orderUID)
	if err != nil {
		return model.Order{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var it model.Item
		if err := rows.Scan(&it.ChrtID, &it.TrackNumber, &it.Price, &it.RID, &it.Name, &it.Sale, &it.Size, &it.TotalPrice, &it.NmID, &it.Brand, &it.Status); err != nil {
			return model.Order{}, false, err
		}
		o.Items = append(o.Items, it)
	}
	return o, true, nil
}

func (r *Repo) LoadAllOrders(ctx context.Context) ([]model.Order, error) {
	rows, err := r.Pool.Query(ctx, `SELECT order_uid FROM orders`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}

	out := make([]model.Order, 0, len(ids))
	for _, id := range ids {
		if o, ok, err := r.GetOrder(ctx, id); err == nil && ok {
			out = append(out, o)
		}
	}
	return out, nil
}
