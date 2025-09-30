// internal/service/service.go
package service

import (
	"context"

	"demo/orders/internal/model"
	"demo/orders/internal/store"
)

type Service struct {
	repo store.Repository
}

func New(repo store.Repository) *Service { return &Service{repo: repo} }

func (s *Service) GetOrder(ctx context.Context, id string) (model.Order, bool, error) {
	return s.repo.GetOrder(ctx, id)
}

func (s *Service) CreateOrUpdateOrder(ctx context.Context, o model.Order) error {
	return s.repo.UpsertOrder(ctx, o)
}
