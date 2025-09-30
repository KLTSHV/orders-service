// internal/service/service_test.go
package service

import (
	"context"
	"testing"

	"demo/orders/internal/model"
	"demo/orders/internal/store/storemock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestService_GetOrder_Found(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := storemock.NewMockRepository(ctrl)
	svc := New(mockRepo)

	expected := model.Order{OrderUID: "o1"}
	mockRepo.EXPECT().GetOrder(gomock.Any(), "o1").Return(expected, true, nil)

	got, ok, err := svc.GetOrder(context.Background(), "o1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, expected, got)
}

func TestService_GetOrder_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := storemock.NewMockRepository(ctrl)
	svc := New(mockRepo)

	mockRepo.EXPECT().GetOrder(gomock.Any(), "nope").Return(model.Order{}, false, nil)
	_, ok, err := svc.GetOrder(context.Background(), "nope")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestService_CreateOrUpdateOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRepo := storemock.NewMockRepository(ctrl)
	svc := New(mockRepo)

	o := model.Order{OrderUID: "x1"}
	mockRepo.EXPECT().UpsertOrder(gomock.Any(), o).Return(nil)

	err := svc.CreateOrUpdateOrder(context.Background(), o)
	require.NoError(t, err)
}
