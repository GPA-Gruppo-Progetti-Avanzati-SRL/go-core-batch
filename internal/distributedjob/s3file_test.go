package distributedjob

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-app/properties"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/internal/scheduler"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/go-core-batch/store"
	"go.uber.org/fx"
)

// Il tag del campo Feed non può nominare la costante: lo blocca questo test.
func TestByS3FileParams_NomeDelFeed(t *testing.T) {
	f, _ := reflect.TypeFor[ByS3FileParams]().FieldByName("Feed")
	if got := f.Tag.Get("name"); got != S3FeedName {
		t.Fatalf("tag name:%q, la costante è %q", got, S3FeedName)
	}
}

type feedStub struct{}

func (feedStub) Feed(context.Context, string, properties.Properties, int) ([]*store.WorkItem, error) {
	return nil, nil
}

// Il job lo registra batch dai jobs:, il feed lo porta s3feed: senza il secondo l'avvio si ferma
// sul Check, che dice cosa wirare.
func TestRegisterByS3File_SenzaFeed(t *testing.T) {
	build := func(extra ...fx.Option) scheduler.JobRegistration {
		var got struct {
			fx.In
			Jobs []scheduler.JobRegistration `group:"batch_jobs"`
		}
		opts := append([]fx.Option{
			fx.NopLogger,
			fx.Provide(func() ITaskDispatcher { return &fakeDispatcher{} }),
			fx.Provide(func() store.IWorkItemStore { return nil }),
			fx.Provide(func() store.IData { return nil }),
			fx.Provide(fx.Annotate(RegisterByS3File, fx.ResultTags(`group:"batch_jobs"`))),
			fx.Populate(&got),
		}, extra...)
		app := fx.New(opts...)
		if err := app.Err(); err != nil {
			t.Fatalf("wiring: %v", err)
		}
		return got.Jobs[0]
	}
	cfg := scheduler.Config{Name: "import-exec", Type: JobTypeByS3File,
		Properties: properties.Properties{"task": "import", "limit": 1}}

	err := build().Check("import-exec", cfg)
	if err == nil || !strings.Contains(err.Error(), "batch.WithS3") {
		t.Fatalf("senza feed: atteso un errore che nomini batch.WithS3, ottenuto %v", err)
	}

	withFeed := build(fx.Provide(fx.Annotate(func() IFeedSource { return feedStub{} },
		fx.ResultTags(`name:"`+S3FeedName+`"`))))
	if err := withFeed.Check("import-exec", cfg); err != nil {
		t.Fatalf("col feed: %v", err)
	}
}
