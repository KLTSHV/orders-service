package service

import (
	"context"
	"testing"

	"demo/orders/internal/model"
	"demo/orders/internal/store"
	"demo/orders/internal/store/storemock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type Service struct {
	repo store.Repository
}

func New(repo store.Repository) *Service {
	return &Service{repo: repo}
}

// методы сервиса (оборачивают вызовы репозитория)
func (s *Service) GetOrder(ctx context.Context, id string) (model.Order, bool, error) {
	return s.repo.GetOrder(ctx, id)
}

func (s *Service) CreateOrder(ctx context.Context, o model.Order) error {
	return s.repo.UpsertOrder(ctx, o)
}

func TestService_GetOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := storemock.NewMockRepository(ctrl)
	svc := New(mockRepo)

	exp := model.Order{OrderUID: "123"}

	mockRepo.EXPECT().
		GetOrder(gomock.Any(), "123").
		Return(exp, true, nil)

	got, ok, err := svc.GetOrder(context.Background(), "123")

	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, exp, got)
}

func TestService_CreateOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := storemock.NewMockRepository(ctrl)
	svc := New(mockRepo)

	o := model.Order{OrderUID: "456"}

	mockRepo.EXPECT().
		UpsertOrder(gomock.Any(), o).
		Return(nil)

	err := svc.CreateOrder(context.Background(), o)
	require.NoError(t, err)
}
