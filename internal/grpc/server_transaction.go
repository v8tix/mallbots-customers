package grpc

import (
	"context"
	"database/sql"

	"google.golang.org/grpc"

	"github.com/v8tix/eda/di"
	"github.com/v8tix/mallbots-customers-proto/pb"
	"github.com/v8tix/mallbots-customers/internal/application"
)

type serverTx struct {
	c di.Container
	pb.UnimplementedCustomersServiceServer
}

var _ pb.CustomersServiceServer = (*serverTx)(nil)

func RegisterServerTx(container di.Container, registrar grpc.ServiceRegistrar) error {
	pb.RegisterCustomersServiceServer(registrar, serverTx{
		c: container,
	})
	return nil
}

func (s serverTx) RegisterCustomer(ctx context.Context, request *pb.RegisterCustomerRequest) (resp *pb.RegisterCustomerResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.RegisterCustomer(ctx, request)
}

func (s serverTx) AuthorizeCustomer(ctx context.Context, request *pb.AuthorizeCustomerRequest) (resp *pb.AuthorizeCustomerResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.AuthorizeCustomer(ctx, request)
}

func (s serverTx) GetCustomer(ctx context.Context, request *pb.GetCustomerRequest) (resp *pb.GetCustomerResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.GetCustomer(ctx, request)
}

func (s serverTx) EnableCustomer(ctx context.Context, request *pb.EnableCustomerRequest) (resp *pb.EnableCustomerResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.EnableCustomer(ctx, request)
}

func (s serverTx) DisableCustomer(ctx context.Context, request *pb.DisableCustomerRequest) (resp *pb.DisableCustomerResponse, err error) {
	ctx = s.c.Scoped(ctx)
	defer func(tx *sql.Tx) {
		err = s.closeTx(tx, err)
	}(di.Get(ctx, "tx").(*sql.Tx))

	next := server{app: di.Get(ctx, "app").(application.App)}

	return next.DisableCustomer(ctx, request)
}

func (s serverTx) closeTx(tx *sql.Tx, err error) error {
	if p := recover(); p != nil {
		_ = tx.Rollback()
		panic(p)
	} else if err != nil {
		_ = tx.Rollback()
		return err
	} else {
		return tx.Commit()
	}
}
